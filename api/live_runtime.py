"""Shared feed/registry/recovery lifecycle, without private operator composition.

Both profiles use the existing state/registry singletons and WebSocket producer.
Only the private profile selects all configured recovery targets by default.
"""

import asyncio
from contextlib import asynccontextmanager
from datetime import datetime
import json
import logging
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, Depends, FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from . import state, worker_registry
from .feed_ingest import broadcast_tick, internal_feed_notify
from .state import last_bar_time, ws_manager

ROOT = Path(__file__).resolve().parent.parent
logger = logging.getLogger(__name__)
_watchdog_backoff: dict[str, float] = {}
_runtime_backoff: dict[str, tuple[float, float]] = {}
api_loop = None
router = APIRouter()


class _WorkerRegisterBody(BaseModel):
    """Existing private worker-registration wire contract."""
    worker_id: str
    pid: int
    port: Optional[int] = None
    mql5_source: Optional[str] = None
    worker_type: Optional[str] = None
    job_id: Optional[str] = None
    agent_name: Optional[str] = None
    started_at: Optional[float] = None
    terminal_id: Optional[str] = None


WorkerRegisterBody = _WorkerRegisterBody  # Preserve the existing private OpenAPI schema name.


@router.post('/internal/worker/register')
async def internal_worker_register(body: WorkerRegisterBody, request: Request):
    """Register into the single registry, applying public-host source restrictions."""
    sources = getattr(request.app.state, 'market_sources', None)
    if sources is not None and (body.mql5_source not in sources
            or body.worker_type not in {'mt5_live', 'mt5_history'}
            or not body.worker_id.startswith(f'{body.worker_type}:')):
        raise HTTPException(403)
    host = getattr(request.app.state, 'mt5_host', None)
    if sources is not None and host is not None:
        from api.mt5_binding import target_revision
        tid = body.terminal_id
        mode = (body.worker_type or '').removeprefix('mt5_')
        try:
            if tid not in request.app.state.recovery_terminal_ids or body.worker_id != f'mt5_{mode}:{tid}':
                raise ValueError('Unselected worker')
            target_revision(host.config_path, tid, require_active=True)
            entries = host.inspect(host.root, tid)
            if host.runtime_managed:
                from api.runtime_session import valid_activation, inspect_bound
                activation = valid_activation(host.root, tid)
                entries = inspect_bound(host, tid, activation['binding'])
                if body.mql5_source != activation['source']:
                    raise ValueError('Worker source does not match activation')
            if not any(entry.pid == body.pid and entry.mode == mode for entry in entries):
                raise ValueError('Unowned worker')
        except (OSError, ValueError):
            raise HTTPException(403) from None
    previous_pid = await worker_registry.get_pid(body.worker_id)
    await worker_registry.register(body.worker_id, body.pid, body.port, body.mql5_source,
        worker_type=body.worker_type, job_id=body.job_id, agent_name=body.agent_name,
        started_at=body.started_at, terminal_id=body.terminal_id)
    if previous_pid != body.pid:
        asyncio.create_task(ws_manager.broadcast({'type': 'brokers_changed'}))
    return {'status': 'registered', 'worker_id': body.worker_id}


@router.post('/internal/worker/status')
async def update_terminal_status(request: Request):
    """Retain private status contract; public hosts accept only fixed recovery IDs."""
    try:
        body = await request.json()
        terminal_id = body.get('terminal_id')
        if not terminal_id:
            return JSONResponse({'ok': False, 'error': 'missing terminal_id'}, status_code=400)
        allowed = getattr(request.app.state, 'recovery_terminal_ids', None)
        if allowed is not None and terminal_id not in allowed:
            raise HTTPException(403)
        await worker_registry.update_terminal_status(terminal_id, body)
        return {'ok': True}
    except HTTPException:
        raise
    except Exception as error:
        logger.error('Failed to update terminal status: %s', error)
        return JSONResponse({'ok': False, 'error': str(error)}, status_code=500)


@router.post('/internal/feed/notify')
async def feed_notify(body: dict, request: Request):
    """Use the one feed producer; public composition admits only selected market data."""
    sources = getattr(request.app.state, 'market_sources', None)
    if sources is not None and (body.get('source') not in sources
            or str(body.get('symbol', '')).upper() not in request.app.state.market_symbols
            or body.get('type') not in {'live_bar', 'forming_bar', 'ticks'}):
        raise HTTPException(403)
    return await internal_feed_notify(body)


@router.websocket('/ws')
async def websocket_endpoint(websocket: WebSocket):
    """Existing event producer; the visitor wrapper owns subscription/output filtering."""
    await ws_manager.connect(websocket)
    try:
        while True:
            data = await websocket.receive_text()
            if data == 'ping':
                await websocket.send_text('pong')
                continue
            try:
                msg = json.loads(data)
                if msg.get('type') == 'subscribe':
                    ws_manager.subscribe(websocket, msg.get('symbols', []))
                elif msg.get('type') == 'unsubscribe':
                    ws_manager.unsubscribe(websocket, msg.get('symbols', []))
            except Exception:
                pass
    except WebSocketDisconnect:
        pass
    finally:
        ws_manager.disconnect(websocket)


async def _bar_missing_scanner() -> None:
    """Preserve the existing feed-warning intervals and forex weekend rule."""
    import time
    await asyncio.sleep(120)
    while True:
        try:
            workers = await worker_registry.all_workers()
            now = time.time()
            utc = datetime.utcnow()
            weekend = utc.weekday() == 5 or (utc.weekday() == 6 and utc.hour < 22)
            for worker_id, entry in workers.items():
                source = entry.get('mql5_source', '')
                if not source or weekend or not await worker_registry.is_alive(worker_id):
                    continue
                last_bar = last_bar_time.get(source)
                age = now - last_bar if last_bar else None
                if last_bar is not None and age >= 90:
                    logger.warning('[BAR FEED] [%s] overdue — last bar %.0fs ago', source, age)
        except Exception:
            pass
        await asyncio.sleep(60)


async def _mt5_watchdog(allowed_terminal_ids=None, *, host=None) -> None:
    """Retain private recovery; public hosts require an explicit fixed target set."""
    await asyncio.sleep(3)
    while True:
        if host is not None and host.runtime_managed:
            # All recovery shares the task route; never spawn in the API's desktop.
            from api.runtime_session import request_launch
            for tid in allowed_terminal_ids or ():
                if (await worker_registry.is_alive(f'mt5_live:{tid}') and
                        await worker_registry.is_alive(f'mt5_history:{tid}')):
                    _runtime_backoff.pop(tid, None)
                    continue
                retry_at, delay = _runtime_backoff.get(tid, (0.0, 5.0))
                if asyncio.get_running_loop().time() < retry_at:
                    continue
                try:
                    launched = await request_launch(host, tid)
                    if launched:
                        # A spawned process is not a connected/registered feed: failed initialization backs off too.
                        _runtime_backoff[tid] = (asyncio.get_running_loop().time() + delay, min(delay * 2, 120.0))
                except Exception:
                    _runtime_backoff[tid] = (asyncio.get_running_loop().time() + delay, min(delay * 2, 120.0))
            await asyncio.sleep(3)
            continue
        # CLI controls run in another process: reconcile their persisted stopped intent here.
        if host is not None:
            for tid in allowed_terminal_ids or ():
                terminal = worker_registry.get_terminal(tid)
                if terminal is not None and terminal.get('active') is False:
                    for mode in ('live', 'history'):
                        await worker_registry.clear(f'mt5_{mode}:{tid}')
        terminal_ids = worker_registry.get_active_terminal_ids() if allowed_terminal_ids != frozenset() else []
        for tid in terminal_ids:
            if allowed_terminal_ids is not None and tid not in allowed_terminal_ids:
                continue
            live_id = f'mt5_live:{tid}'
            if not await worker_registry.is_alive(live_id):
                pid = await worker_registry.get_pid(live_id)
                if pid is None:
                    pid = await worker_registry.get_pid(f'mt5_history:{tid}')
                if pid is not None:
                    backoff = _watchdog_backoff.get(tid, 5.0)
                    logger.warning('MT5 live worker [%s] (pid=%s) not found — respawning in %.0fs', tid, pid, backoff)
                    await asyncio.sleep(backoff)
                    _watchdog_backoff[tid] = min(backoff * 2, 120.0)
                    await _respawn_terminal_worker(tid, host=host)
                else:
                    _watchdog_backoff.pop(tid, None)
            else:
                _watchdog_backoff.pop(tid, None)
                if not await worker_registry.is_alive(f'mt5_history:{tid}'):
                    await _respawn_terminal_worker(tid, host=host)
        await asyncio.sleep(3)


async def _respawn_terminal_worker(terminal_id: str, *, host=None) -> None:
    """Recheck private intent under the shared fence, then fill missing modes only."""
    from api.mt5_host import MT5Host
    from api.mt5_binding import target_revision
    from shared.mt5_ownership import operation_fence
    host = host if host is not None else MT5Host(ROOT)
    try:
        if host.runtime_managed:
            from api.runtime_session import request_launch
            await request_launch(host, terminal_id)
            return
        with operation_fence(host.root):
            target_revision(host.config_path, terminal_id, require_active=True)
            host.start_pair(terminal_id)
        logger.info('MT5 workers [%s] recovery checked', terminal_id)
    except Exception as error:
        logger.error('Failed to respawn MT5 workers [%s]: %s', terminal_id, error)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Own exactly one tracker/watchdog/scanner and join them on every exit path."""
    global api_loop
    if state.feed_tracker is not None:
        raise RuntimeError('A live runtime already owns this process')
    public = getattr(app.state, 'market_sources', None) is not None
    if public and (state.live_tick_callback is not None or state.live_bar_callback is not None):
        raise RuntimeError('Public live runtime cannot install private callbacks')
    from live.zmq_feed import FeedHealthTracker
    tracker = FeedHealthTracker(broadcast_fn=broadcast_tick)
    tasks = []
    state.feed_tracker = tracker
    api_loop = asyncio.get_running_loop()
    try:
        await tracker.start()
        tasks.append(asyncio.create_task(_mt5_watchdog(getattr(app.state, 'recovery_terminal_ids', None),
            host=getattr(app.state, 'mt5_host', None)), name='mt5_watchdog'))
        tasks.append(asyncio.create_task(_bar_missing_scanner(), name='bar_missing_scanner'))
        yield
    finally:
        async def cleanup():
            """Retain lifecycle ownership even if shutdown is cancelled repeatedly."""
            global api_loop
            for task in tasks:
                task.cancel()
            try:
                await asyncio.gather(*tasks, return_exceptions=True)
                await tracker.stop()
            finally:
                state.feed_tracker = None
                api_loop = None
        closing = asyncio.create_task(cleanup())
        interrupted = False
        while True:
            try:
                await asyncio.shield(closing)
                break
            except asyncio.CancelledError:
                if closing.cancelled():
                    raise
                interrupted = True
        if interrupted:
            raise asyncio.CancelledError


async def _loopback_callback(request: Request):
    """Internal HTTP callbacks are host-local, not public or authentication endpoints."""
    if request.client is None or request.client.host not in {'127.0.0.1', '::1'}:
        raise HTTPException(403)


def create_live_app(policy, *, recovery_terminal_ids=()):
    """Construct a callback/WS host without private routers, jobs or .env loading.

    An empty target set is deliberately inert. A trusted host must inject an
    approved recovery target set separately; source aliases never select targets.
    """
    import re
    targets = frozenset(recovery_terminal_ids)
    if any(type(tid) is not str or re.fullmatch(r'term_[0-9a-f]{8}', tid) is None for tid in targets):
        raise ValueError('Invalid fixed recovery target')
    app = FastAPI(lifespan=lifespan, docs_url=None, redoc_url=None, openapi_url=None)
    app.state.market_sources = frozenset(policy.sources.values())
    app.state.market_symbols = frozenset(policy.symbols)
    app.state.recovery_terminal_ids = targets
    # Dependencies on the HTTP-only callback routes leave the WS producer usable
    # through the visitor wrapper with the original external client address.
    callbacks = APIRouter(dependencies=[Depends(_loopback_callback)])
    for path, function in [('/internal/worker/register', internal_worker_register),
                           ('/internal/worker/status', update_terminal_status),
                           ('/internal/feed/notify', feed_notify)]:
        callbacks.add_api_route(path, function, methods=['POST'])
    app.include_router(callbacks)
    app.add_api_websocket_route('/ws', websocket_endpoint)
    return app
