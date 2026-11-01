"""
live/tick_store.py
==================
In-memory ring buffer for live ticks, keyed by (symbol, source).

Each MT5 instance (primary, secondary, ...) stores its ticks separately,
enabling side-by-side comparison and per-source operations.

Thread-safe — per-key locks protect per-symbol data; _meta_lock protects
shared metadata (_sources, _locks dict).  This replaces the previous single
global lock, eliminating contention across symbols at high tick rates.

Each buffer holds at most MAXLEN ticks per (symbol, source) pair;
oldest entries are evicted automatically (deque maxlen).

Tick format stored:
    {"symbol": str, "bid": float, "ask": float, "time_ms": int, "source": str}
"""

from __future__ import annotations

import statistics
import threading
import time
from collections import deque
from typing import Optional

MAXLEN = 2000  # ticks retained per (symbol, source)

_store: dict[str, deque] = {}   # key: "SYMBOL:source"
_sources: set[str] = set()

# Per-key locks — one lock per (symbol:source) pair; created lazily.
# _meta_lock protects _sources, _locks dict creation, and global iterations.
_locks: dict[str, threading.Lock] = {}
_meta_lock = threading.Lock()


def _get_lock(key: str) -> threading.Lock:
    """Return (and lazily create) the per-key lock for a (symbol:source) key."""
    with _meta_lock:
        if key not in _locks:
            _locks[key] = threading.Lock()
        return _locks[key]

# 1-in-100 sampling counters to avoid flooding metrics_store at ~480 ticks/s
_sample_counter: dict[str, int] = {}

# Cycle monitoring — per (symbol:source)
_tick_minute_counts: dict[str, dict[int, int]] = {}   # {minute_floor: count}
_tick_latencies: dict[str, deque] = {}                # deque(maxlen=200) of ms
_tick_to_api_latencies: dict[str, deque] = {}        # deque(maxlen=200) of ms


def append_tick(symbol: str, source: str, tick: dict) -> None:
    """Append one tick to the ring buffer for (symbol, source)."""
    key = f"{symbol}:{source}"
    now = time.time()
    minute_floor = int(now // 60) * 60

    # Register source under meta lock (infrequent — only on first tick per source)
    with _meta_lock:
        _sources.add(source)

    lock = _get_lock(key)
    with lock:
        if key not in _store:
            _store[key] = deque(maxlen=MAXLEN)
        _store[key].append(tick)
        _sample_counter[key] = (_sample_counter.get(key, 0) + 1) % 100

        # Per-minute count for sparkline
        counts = _tick_minute_counts.setdefault(key, {})
        counts[minute_floor] = counts.get(minute_floor, 0) + 1
        cutoff_min = int((now - 3900) // 60) * 60  # keep 65 min
        # Replace dict to drop old keys — avoids temporary list allocation
        if any(k < cutoff_min for k in counts):
            _tick_minute_counts[key] = {k: v for k, v in counts.items() if k >= cutoff_min}

        # Tick pipeline latency — Python-side only (MT5 TimeGMT() has only second
        # precision so any MT5-originated timestamp has ±1000 ms rounding error).
        time_ms = tick.get("time_ms", 0)
        if time_ms and time_ms > 0:
            # T→API: broker tick timestamp → Python POST receipt (true end-to-end freshness)
            push_received_ms = tick.get("push_received_ms", 0)
            receipt_ms = push_received_ms if push_received_ms else now * 1000
            t_to_api = max(0.0, receipt_ms - time_ms)
            if key not in _tick_to_api_latencies:
                _tick_to_api_latencies[key] = deque(maxlen=200)
            _tick_to_api_latencies[key].append(t_to_api)

            # API→Engine: time from POST receipt → tick_store.append_tick call
            if push_received_ms and push_received_ms > 0:
                api_engine_ms = max(0.0, now * 1000 - push_received_ms)
            else:
                api_engine_ms = 0.0
            if key not in _tick_latencies:
                _tick_latencies[key] = deque(maxlen=200)
            _tick_latencies[key].append(api_engine_ms)


def get_tick_sparkline(symbol: str, source: str, minutes: int = 60) -> list:
    """Return per-minute tick counts for the last `minutes` minutes (oldest first)."""
    key = f"{symbol}:{source}"
    now = time.time()
    lock = _get_lock(key)
    with lock:
        counts = dict(_tick_minute_counts.get(key, {}))
    return [
        counts.get(int((now - (minutes - 1 - i) * 60) // 60) * 60, 0)
        for i in range(minutes)
    ]


def get_tick_engine_latency(symbol: str, source: str) -> dict:
    """Return last and average API→Engine tick latency in ms."""
    key = f"{symbol}:{source}"
    lock = _get_lock(key)
    with lock:
        lats = list(_tick_latencies.get(key, []))
    if not lats:
        return {"last_ms": None, "avg_ms": None}
    return {
        "last_ms": round(lats[-1], 1),
        "avg_ms": round(statistics.mean(lats), 1),
    }


def get_tick_to_api_latency(symbol: str, source: str) -> dict:
    """Return last and average Tick→API latency in ms (broker tick time → MT5 POST)."""
    key = f"{symbol}:{source}"
    lock = _get_lock(key)
    with lock:
        lats = list(_tick_to_api_latencies.get(key, []))
    if not lats:
        return {"last_ms": None, "avg_ms": None}
    return {
        "last_ms": round(lats[-1], 1),
        "avg_ms": round(statistics.mean(lats), 1),
    }


def reset_latencies() -> None:
    """Clear all latency deques so averages start fresh."""
    with _meta_lock:
        keys = list(_locks.keys())
    for key in keys:
        with _get_lock(key):
            _tick_latencies.pop(key, None)
            _tick_to_api_latencies.pop(key, None)


def get_ticks(
    symbol: str,
    source: str,
    since_ms: int = 0,
    limit: int = 500,
) -> list[dict]:
    """Return up to `limit` ticks for (symbol, source) after `since_ms`.

    Args:
        symbol:   Currency pair, e.g. ``"EURUSD"``.
        source:   Bridge source name, e.g. ``"primary"``.
        since_ms: Return only ticks with ``time_ms > since_ms``.
        limit:    Maximum number of ticks to return (most recent).
    """
    key = f"{symbol}:{source}"
    lock = _get_lock(key)
    with lock:
        buf = _store.get(key)
        if buf is None:
            return []
        ticks = [t for t in buf if t.get("time_ms", 0) > since_ms]
    return ticks[-limit:]


def get_latest(symbol: str, source: str) -> Optional[dict]:
    """Return the single most recent tick for (symbol, source), or None."""
    key = f"{symbol}:{source}"
    lock = _get_lock(key)
    with lock:
        buf = _store.get(key)
        if not buf:
            return None
        return dict(buf[-1])


def get_sources() -> list[str]:
    """Return sorted list of all source names seen so far."""
    with _meta_lock:
        return sorted(_sources)


def get_symbols(source: str) -> list[str]:
    """Return symbols that have received at least one tick from `source`."""
    with _meta_lock:
        keys = list(_store.keys())
    return sorted(
        k.split(":")[0]
        for k in keys
        if k.endswith(f":{source}") and _store.get(k)
    )


def compare(symbol: str) -> dict[str, Optional[dict]]:
    """Return the latest tick from every known source for one symbol.

    Returns:
        ``{"primary": {...}, "secondary": {...}, ...}``
    """
    with _meta_lock:
        sources = sorted(_sources)
    return {src: get_latest(symbol, src) for src in sources}
