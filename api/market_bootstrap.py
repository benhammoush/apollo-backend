"""Narrow public bootstrap coordinator for one server-bound MT5 live/history pair.

The host supplies fixed-target, read-only inspection and atomic pair-start
callbacks. No client arguments, process launching, discovery or private settings
are accepted here. One coordinator must be shared by the owning runtime.
"""

import asyncio
from collections.abc import Awaitable, Callable
import time


class MarketFeedBootstrap:
    """Coalesce requests and retain launch ownership beyond HTTP cancellation.

    Inspection must include pending/unregistered children and return stopped only
    when both approved workers are confirmed absent. The host's atomic launcher
    must share ownership with recovery/private controls; unavailable/partial
    states never permit public startup. This class alone is not that host binding.
    """

    def __init__(self, source_alias: str, inspect_pair: Callable[[], Awaitable[str]],
                 launch_pair: Callable[[], Awaitable[None]], *, cooldown_seconds: int = 60,
                 clock: Callable[[], float] = time.monotonic):
        """Bind trusted zero-argument callbacks once, never from visitor input."""
        if not isinstance(source_alias, str) or not source_alias or not source_alias.replace('-', '').replace('_', '').isalnum():
            raise ValueError("Explicit approved source alias required")
        if type(cooldown_seconds) is not int or not 60 <= cooldown_seconds <= 3600:
            raise ValueError("Invalid bootstrap cooldown")
        if not callable(inspect_pair) or not callable(launch_pair):
            raise ValueError("Explicit fixed-target callbacks required")
        self._source_alias = source_alias
        self._inspect_pair = inspect_pair
        self._launch_pair = launch_pair
        self._cooldown = cooldown_seconds
        self._clock = clock
        self._retry_at = 0.0
        self._lock = asyncio.Lock()
        self._launch_task = None
        self._closing = False

    @property
    def source_alias(self) -> str:
        """Expose only the already-approved alias for server-owned policy validation."""
        return self._source_alias

    def _response(self, state: str) -> dict:
        """Return a small fixed schema with no PIDs, paths, target settings or errors."""
        retry = max(0, int(self._retry_at - self._clock() + 0.999))
        return {"enabled": True, "state": state, "can_start": state == "stopped",
                "retry_after_seconds": retry if state == "cooldown" else 0}

    async def _state(self) -> str:
        """Fail closed on unknown, stale, failed or timed-out inspection."""
        try:
            state = await asyncio.wait_for(self._inspect_pair(), timeout=2)
        except Exception:
            return "unavailable"
        return state if type(state) is str and state in {"stopped", "starting", "running", "partial", "unavailable"} else "unavailable"

    async def status(self) -> dict:
        """Inspection never starts workers or performs automatic public retries."""
        if self._launch_task is not None and not self._launch_task.done():
            return self._response("starting")
        if self._closing:
            return self._response("unavailable")
        state = await self._state()
        if self._closing:
            return self._response("unavailable")
        if state == "stopped" and self._clock() < self._retry_at:
            state = "cooldown"
        return self._response(state)

    async def _launch(self):
        """Keep one real launch task alive after callers time out/disconnect."""
        try:
            await self._launch_pair()
        except Exception:
            # The host owns private diagnostics. Never retain/return raw errors.
            pass

    async def start(self) -> dict:
        """Recheck confirmed absence under a lock and schedule at most one attempt."""
        async with self._lock:
            current = await self.status()
            if self._closing:
                return self._response("unavailable")
            if not current["can_start"]:
                return current
            # Charge cooldown before launching, including failed attempts. An
            # in-flight launch remains starting even when this cooldown elapses.
            self._retry_at = self._clock() + self._cooldown
            self._launch_task = asyncio.create_task(self._launch())
            return self._response("starting")

    async def drain(self):
        """Host shutdown may await owned launch work; client cancellation cannot stop it."""
        if self._launch_task is not None:
            await asyncio.shield(self._launch_task)

    async def close(self):
        """Seal new requests and retain shutdown ownership until the attempt finishes."""
        self._closing = True
        interrupted = False
        while self._launch_task is not None and not self._launch_task.done():
            try:
                await asyncio.shield(self._launch_task)
            except asyncio.CancelledError:
                if self._launch_task.cancelled():
                    raise
                interrupted = True
        if interrupted:
            raise asyncio.CancelledError
