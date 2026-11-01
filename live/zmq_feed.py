"""
live/zmq_feed.py
================
Feed health tracker for the MT5 HTTP bridge.

apollo_bridge.mq5 POSTs batches directly to POST /internal/feed/notify.
This module only tracks per-symbol, per-source health (last seen time,
staleness) — no network sockets.
"""

from __future__ import annotations

import time
from datetime import datetime, timezone

_STALE_SECONDS = 180  # 3 minutes without a tick → stale


class FeedHealthTracker:
    """Tracks per-symbol, per-source feed health.
    No network sockets — HTTP bridge posts directly to FastAPI.
    """

    def __init__(self, broadcast_fn=None):
        self._broadcast_fn = broadcast_fn
        # key: "SYMBOL:source"  value: monotonic timestamp
        self._last_seen: dict[str, float] = {}

    async def start(self) -> None:
        pass  # nothing to start

    async def stop(self) -> None:
        pass

    def record(self, symbol: str, source: str = "primary") -> None:
        """Mark (symbol, source) as seen now."""
        self._last_seen[f"{symbol}:{source}"] = time.time()

    def is_stale(self, symbol: str, source: str = "primary") -> bool:
        ts = self._last_seen.get(f"{symbol}:{source}")
        return ts is None or (time.time() - ts) > _STALE_SECONDS

    def status(self) -> dict:
        """Return nested dict: {symbol: {source: {age_seconds, stale, ...}}}"""
        now = time.time()
        result: dict = {}
        for key, ts in self._last_seen.items():
            sym, src = key.split(":", 1)
            result.setdefault(sym, {})[src] = {
                "last_seen_unix": ts,
                "last_seen_iso":  datetime.fromtimestamp(ts, tz=timezone.utc).isoformat(),
                "stale":          (now - ts) > _STALE_SECONDS,
                "age_seconds":    round(now - ts, 1),
            }
        return result
