"""Local visitor candidate entry point; no deployment or public binding is implied.

Fixture: python run_read_only.py --fixture --port 8811 --origin http://127.0.0.1:5181
Live configuration must be explicitly supplied. The callback-only listener keeps
port 8000 for existing workers, bound to loopback only in this mode.
"""

import argparse
import asyncio
import json
from contextlib import contextmanager
from pathlib import Path
import signal
import threading


@contextmanager
def _host_signals(stopping):
    """One host owns signals until launches/requests/callbacks have all drained.

    Independent Uvicorn signal owners could tear the callback runtime down before
    the visitor's retained handlers finish. Embedded non-main-thread callers own
    their explicit stop event instead. Repeated signals never abandon ownership.
    """
    previous = {}
    if threading.current_thread() is threading.main_thread():
        signals = [signal.SIGINT, signal.SIGTERM]
        if hasattr(signal, 'SIGBREAK'):
            signals.append(signal.SIGBREAK)
        def request_shutdown(*_):
            stopping.set()
        try:
            for item in signals:
                previous[item] = signal.signal(item, request_shutdown)
            yield
        finally:
            for item, handler in previous.items():
                signal.signal(item, handler)
    else:
        yield


def load_policy(path: Path):
    """Load an explicitly supplied selection file, not guessed workspace configuration."""
    from api.read_only import ReadOnlyPolicy, SelectedResult
    from api.result_contracts import DatasetDescriptor
    data = json.loads(path.read_text(encoding="utf-8"))
    root = (path.parent / data.pop("result_root")).resolve()
    selected = []
    for item in data.pop("results", []):
        selected.append(SelectedResult(DatasetDescriptor(**{
            **item["description"], "symbols": tuple(item["description"]["symbols"]),
            "timeframes": tuple(item["description"]["timeframes"]),
            **({"columns": tuple(item["description"]["columns"])} if "columns" in item["description"] else {}),
            **({"date_columns": tuple(item["description"]["date_columns"])}
               if item["description"].get("date_columns") is not None else {}),
        }), root / item["file"]))
    return ReadOnlyPolicy(**{**data, "symbols": tuple(data["symbols"]), "origins": tuple(data["origins"]),
                             "result_root": root, "results": tuple(selected)})


async def serve_live(policy, port, market_starter=None, *, recovery_terminal_ids=(),
                      callback_port=8000, stop_event=None, operator_app=None, mt5_host=None):
    """Serve one runtime owner and one visitor facade, with host-local injection only.

    The private launcher may inject its existing operator app. The standalone
    public entrypoint constructs only the restricted callback runtime instead.
    """
    # A JSON capability flag alone must not activate spawning or even import the
    # private runtime on an invalid/unbound configuration. Host binding is separate.
    from api.read_only import validate_market_start_binding
    validate_market_start_binding(policy, market_starter)
    if (type(port) is not int or type(callback_port) is not int or port == callback_port
            or not 1024 <= port <= 65535 or not 1024 <= callback_port <= 65535):
        raise ValueError('Distinct nonprivileged loopback listener ports required')
    if market_starter is not None and not recovery_terminal_ids:
        raise ValueError('Public startup requires an explicit fixed recovery scope')
    import uvicorn
    from api.live_runtime import create_live_app, websocket_endpoint
    from api.read_only import ReadOnlyApplication
    from api.read_only_live import LiveReadServices
    from starlette.routing import Router, WebSocketRoute
    class ManagedServer(uvicorn.Server):
        """The host, not either individual listener, owns ordered signal teardown."""
        @contextmanager
        def capture_signals(self):
            yield
    app = operator_app if operator_app is not None else create_live_app(policy, recovery_terminal_ids=recovery_terminal_ids)
    if mt5_host is not None:
        app.state.mt5_host = mt5_host
    services = LiveReadServices(policy)
    # Share the existing event producer, never forward into private app middleware/routes.
    events = Router(routes=[WebSocketRoute("/ws", websocket_endpoint)])
    public = ReadOnlyApplication(policy, services, events, market_starter=market_starter)
    servers = [
        ManagedServer(uvicorn.Config(app, host="127.0.0.1", port=callback_port, proxy_headers=False)),
        ManagedServer(uvicorn.Config(public, host="127.0.0.1", port=port, lifespan="off", ws_max_size=2048, proxy_headers=False)),
    ]
    stopping = stop_event if stop_event is not None else asyncio.Event()
    unexpected_exit = False
    with _host_signals(stopping):
        tasks = [asyncio.create_task(server.serve()) for server in servers]
        stop = asyncio.create_task(stopping.wait())
        try:
            await asyncio.wait([*tasks, stop], return_when=asyncio.FIRST_COMPLETED)
            unexpected_exit = not stopping.is_set()
        finally:
            async def shutdown():
                """Drain launches, stop visitor admission/requests, then retire callbacks."""
                try:
                    if market_starter is not None:
                        await market_starter.close()
                finally:
                    try:
                        public.seal()
                        servers[1].should_exit = True
                        await asyncio.gather(tasks[1], return_exceptions=True)
                        await public.close()
                        if operator_app is None:
                            await services.close()
                    finally:
                        servers[0].should_exit = True
                        await asyncio.gather(tasks[0], return_exceptions=True)
                        if operator_app is not None:
                            # Private requests share the history transport; drain them first.
                            await services.close()
                        stop.cancel()
                        await asyncio.gather(stop, return_exceptions=True)
            closing = asyncio.create_task(shutdown())
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
        for task in tasks:
            if not task.cancelled() and task.exception() is not None:
                raise task.exception()
        if unexpected_exit:
            raise RuntimeError('Backend listener exited without a requested shutdown')


def main():
    """Require fixture mode or an explicit selection; never reuse an existing listener."""
    parser = argparse.ArgumentParser(description=__doc__)
    modes = parser.add_mutually_exclusive_group(required=True)
    modes.add_argument("--fixture", action="store_true")
    modes.add_argument("--config", type=Path)
    parser.add_argument("--port", type=int, default=8001)
    parser.add_argument("--origin", action="append", default=[])
    args = parser.parse_args()
    if not 1024 <= args.port <= 65535 or args.port == 8000:
        parser.error("Choose a distinct nonprivileged visitor port")
    if args.fixture:
        if not args.origin:
            parser.error("Fixture verification requires explicit --origin")
        import uvicorn
        from api.read_only_fixture import fixture_application
        with fixture_application(args.origin) as app:
            uvicorn.run(app, host="127.0.0.1", port=args.port, ws_max_size=2048)
    else:
        if args.origin:
            parser.error("Live origins belong in the explicit selection file")
        try:
            policy = load_policy(args.config.resolve())
        except (OSError, ValueError, TypeError, KeyError):
            parser.error("Invalid or unavailable visitor selection configuration")
        asyncio.run(serve_live(policy, args.port))


if __name__ == "__main__":
    main()
