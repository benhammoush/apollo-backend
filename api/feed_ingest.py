"""
api/feed_ingest.py
==================
Internal feed notification handler.
Extracted from main.py to break circular imports.
"""
from __future__ import annotations

import asyncio
import logging
import time as _time
from datetime import datetime, timezone
from typing import Any

from . import state
from .state import (
    last_push_time,
    last_bar_time,
    last_tick_time,
    last_bar_time_sym,
    last_forming_time_sym,
    paused_sources,
    ws_manager,
)
from live import tick_store

logger = logging.getLogger(__name__)


def _broadcast_payload(event_type: str, symbol: str, extra: dict | None = None) -> dict:
    """Build a standard broadcast payload dict."""
    payload: dict[str, Any] = {"type": event_type}
    if extra:
        payload.update(extra)
    return payload


async def _broadcast(ws, event_type: str, symbol: str, extra: dict | None = None) -> None:
    """Broadcast a typed event to all subscribed WebSocket clients."""
    payload = _broadcast_payload(event_type, symbol, extra)
    await ws.broadcast(payload, symbol=symbol)


async def broadcast_tick(
    symbol: str,
    bid: float,
    ask: float,
    source: str = "primary",
    time_ms: int = 0,
) -> None:
    """Broadcast a live tick to all connected WebSocket clients."""
    await _broadcast(ws_manager, "tick", symbol, {
        "data": {
            "symbol": symbol,
            "bid": bid,
            "ask": ask,
            "source": source,
            "time_ms": time_ms,
            "time": datetime.utcnow().isoformat(),
        },
    })


async def internal_feed_notify(body: dict):
    """Receive live events POSTed by the MT5 worker. WS broadcast only — no storage.

    Private synchronous callbacks are disabled by default and may be installed
    explicitly in api.state by a trusted private profile. Market health and
    delivery do not depend on a callback being present or successful.

    Body types:
      {"type": "live_bar",    "source": ..., "symbol": ..., "bar": {time, open, high, low, close, volume}}
      {"type": "forming_bar", "source": ..., "symbol": ..., "bar_open_time": ..., open, high, low, close, volume}
      {"type": "ticks",       "source": ..., "symbol": ..., "ticks": [{time_ms, bid, ask}, ...]}
    """
    source = body.get("source", "")
    symbol = body.get("symbol", "").upper()
    msg_type = body.get("type", "")

    last_push_time[source] = _time.time()

    if source in paused_sources:
        return {"ok": True, "source": source, "paused": True}

    if msg_type == "live_bar":
        bar = body.get("bar", {})
        if state.feed_tracker is not None:
            state.feed_tracker.record(symbol, source)
        _bar_actual_ts = bar.get("time")
        if _bar_actual_ts:
            try:
                last_bar_time[source] = datetime.fromisoformat(
                    _bar_actual_ts.replace("Z", "+00:00")
                ).timestamp()
            except Exception:
                last_bar_time[source] = _time.time()
        else:
            last_bar_time[source] = _time.time()
        last_bar_time_sym[f"{source}:{symbol}"] = _time.time()
        callback = state.live_bar_callback
        if callback is not None:
            try:
                callback(
                    symbol=symbol,
                    time=datetime.fromisoformat(bar["time"]).replace(tzinfo=timezone.utc)
                         if bar.get("time") else datetime.now(timezone.utc),
                    open=float(bar.get("open", 0)),
                    high=float(bar.get("high", 0)),
                    low=float(bar.get("low", 0)),
                    close=float(bar.get("close", 0)),
                    volume=int(bar.get("volume", 0)),
                    source=source,
                )
            except Exception as e:
                logger.warning("on_live_bar failed for %s (%s): %s", symbol, type(e).__name__, e)
        # Convert ISO time to integer Unix timestamp so the frontend
        # receives the same type as historical bars (LWC rejects mixed types).
        _bar_ts_int = int(datetime.fromisoformat(
            bar["time"].replace("Z", "+00:00")
        ).timestamp()) if bar.get("time") else int(_time.time())
        asyncio.create_task(_broadcast(ws_manager, "live_bar", symbol, {
            "source": source,
            "has_bars": True,
            "symbols": [symbol],
            "bar_time": _bar_ts_int,
            "bars": [{
                "symbol": symbol,
                "time":   _bar_ts_int,
                "open":   round(float(bar.get("open",  0)), 5),
                "high":   round(float(bar.get("high",  0)), 5),
                "low":    round(float(bar.get("low",   0)), 5),
                "close":  round(float(bar.get("close", 0)), 5),
                "volume": int(bar.get("volume", 0)),
            }],
            "received_at": datetime.now().isoformat(timespec="milliseconds"),
        }))

    elif msg_type == "forming_bar":
        last_forming_time_sym[f"{source}:{symbol}"] = _time.time()
        asyncio.create_task(_broadcast(ws_manager, "forming_bar", symbol, {
            "source":       source,
            "symbol":       symbol,
            "bar_open_time": body.get("bar_open_time"),
            "open":  round(float(body.get("open",  0)), 5),
            "high":  round(float(body.get("high",  0)), 5),
            "low":   round(float(body.get("low",   0)), 5),
            "close": round(float(body.get("close", 0)), 5),
            "volume": body.get("volume", 0),
        }))

    elif msg_type == "ticks":
        ticks = body.get("ticks", [])
        if state.feed_tracker is not None:
            state.feed_tracker.record(symbol, source)
        push_received_ms = _time.time() * 1000
        latest: dict | None = None
        for tick in ticks:
            entry = {
                "symbol":          symbol,
                "bid":             float(tick["bid"]),
                "ask":             float(tick["ask"]),
                "time_ms":         int(tick.get("time_ms", 0)),
                "source":          source,
                "push_received_ms": push_received_ms,
            }
            tick_store.append_tick(symbol, source, entry)
            latest = entry

        if latest:
            last_tick_time[source] = _time.time()
            callback = state.live_tick_callback
            if callback is not None:
                try:
                    callback(symbol=symbol, bid=latest["bid"], ask=latest["ask"],
                             time_ms=latest["time_ms"], source=source)
                except Exception as e:
                    logger.warning("on_live_tick failed for %s (%s): %s", symbol, type(e).__name__, e)
            asyncio.create_task(broadcast_tick(symbol, latest["bid"], latest["ask"],
                                               source=source, time_ms=latest["time_ms"]))
            asyncio.create_task(_broadcast(ws_manager, "live_tick", symbol, {
                "source":    source,
                "has_ticks": True,
                "sample":    {"symbol": symbol,
                              "bid": round(latest["bid"], 5),
                              "ask": round(latest["ask"], 5)},
                "count":     len(ticks),
                "received_at": datetime.now().isoformat(timespec="milliseconds"),
            }))

    return {"ok": True, "source": source, "type": msg_type}
