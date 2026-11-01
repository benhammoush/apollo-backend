"""Selected-source market adapter sharing Apollo's existing worker registry.

No private execution, source fallback, terminal management or worker spawning is
performed here. The operational listener retains ingestion/watchdog ownership.
"""

import asyncio
from collections import OrderedDict
from datetime import datetime, timedelta, timezone
import json

from fastapi import HTTPException


class LiveReadServices:
    """Bounded visitor reads using exact registered history workers only."""

    def __init__(self, policy):
        """Keep a small, expiring, source-separated visitor-only result cache."""
        self.policy = policy
        self._cache = OrderedDict()
        self._pending = {}
        self._closing = False

    async def available_sources(self):
        """Return exact live registry identities without exposing diagnostics."""
        if self._closing:
            raise HTTPException(503)
        from api import worker_registry
        workers = await worker_registry.all_workers()
        sources = set()
        for worker_id, worker in workers.items():
            source = worker.get("mql5_source")
            if worker.get("worker_type") == "mt5_history" and source in self.policy.sources.values():
                if await worker_registry.is_alive(worker_id):
                    sources.add(source)
        return sources

    async def _port(self, source):
        """Never call the permissive get-port-by-source/legacy PID-file fallbacks."""
        from api import worker_registry
        workers = await worker_registry.all_workers()
        for worker_id, worker in workers.items():
            if worker.get("worker_type") == "mt5_history" and worker.get("mql5_source") == source:
                if await worker_registry.is_alive(worker_id):
                    return worker["port"]
        raise HTTPException(503)

    async def _fetch(self, symbol, tf, source, start, end):
        """Bound worker bytes/rows before decoding; use its existing serialized access."""
        from api.market_read import _get_client, _decode_ohlcv_response
        port = await self._port(source)
        date_from = datetime.fromisoformat(start).replace(tzinfo=timezone.utc)
        date_to = datetime.fromisoformat(end).replace(tzinfo=timezone.utc) + timedelta(days=1) - timedelta(seconds=1)
        params = {"from_date": date_from.isoformat(), "to_date": date_to.isoformat()}
        raw = bytearray()
        async with _get_client().stream("GET", f"http://127.0.0.1:{port}/history/{symbol}/{tf}", params=params) as response:
            response.raise_for_status()
            async for chunk in response.aiter_bytes():
                if len(raw) + len(chunk) > self.policy.max_bytes:
                    raise HTTPException(413)
                raw.extend(chunk)
        envelope = json.loads(raw)
        if envelope.get("symbol") != symbol or envelope.get("tf") != tf:
            raise HTTPException(503)
        count = envelope.get("count") if envelope.get("format") == "parquet_b64" else len(envelope.get("bars", []))
        if type(count) is not int or not 0 <= count <= self.policy.max_rows:
            raise HTTPException(413)
        rows = await asyncio.to_thread(_decode_ohlcv_response, bytes(raw))
        if len(rows) > self.policy.max_rows:
            raise HTTPException(413)
        return rows

    async def market(self, symbol, tf, source, start, end):
        """Deduplicate reads and bound cache lifetime/cardinality, without empty retries."""
        if self._closing:
            raise HTTPException(503)
        if source not in self.policy.sources.values():
            raise HTTPException(404)
        # Availability is checked even for cache hits; never silently serve another source.
        await self._port(source)
        if self._closing:
            raise HTTPException(503)
        now = asyncio.get_running_loop().time()
        for key in list(self._cache):
            if self._cache[key][0] <= now:
                del self._cache[key]
        key = (symbol, tf, source, start, end)
        if key in self._cache:
            return self._cache[key][1]
        if key not in self._pending:
            if len(self._pending) >= self.policy.max_inflight:
                raise HTTPException(429)
            # The HTTP boundary times out the waiter, not this actual operation.
            # Cancelling _fetch could detach a still-running decoder thread and
            # falsely free its admission. Network I/O retains the client's own
            # timeout/byte limits; completion releases the charged work slot.
            task = asyncio.create_task(self._fetch(symbol, tf, source, start, end))
            self._pending[key] = task

            def complete(done):
                """Retire bounded work even if its original HTTP client disconnected."""
                if self._pending.get(key) is done:
                    self._pending.pop(key, None)
                if not done.cancelled():
                    error = done.exception()
                    if error is None:
                        self._cache[key] = (asyncio.get_running_loop().time() + 5, done.result())
                        while len(self._cache) > 4:
                            self._cache.popitem(last=False)

            task.add_done_callback(complete)
        task = self._pending[key]
        return await asyncio.shield(task)

    async def close(self):
        """Seal admission and drain network/decode work before transport teardown."""
        self._closing = True
        pending = tuple(self._pending.values())
        interrupted = False
        if pending:
            drain = asyncio.gather(*pending, return_exceptions=True)
            while True:
                try:
                    await asyncio.shield(drain)
                    break
                except asyncio.CancelledError:
                    interrupted = True
        from api.market_read import close_client
        await close_client()
        self._cache.clear()
        if interrupted:
            raise asyncio.CancelledError
