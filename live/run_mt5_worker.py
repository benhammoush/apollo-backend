"""
live/run_mt5_worker.py — shared market worker
=========================
Unified MT5 terminal worker — one instance per configured terminal.
This is a host-local protocol, not a public HTTP application. The private
compatibility entrypoint installs trading/job-export support and private settings;
the shared worker imports neither integration.

Supports three modes via --mode flag:

  live      (default) — tick loop + bar loop + broadcast to /internal/feed/notify
                        NEVER calls copy_rates_range()
                        Registers as mt5_live:{terminal_id}

  history   — only serves /history/{symbol}/{tf} (copy_rates_range + LRU cache)
              No loops, no broadcasting.  Serves frontend charts.
              Registers as mt5_history:{terminal_id}

  ephemeral — full worker (loops + /history/), does NOT broadcast to API.
              Private to one agent run.  Spawned by launcher.py.
              Registers as mt5_ephemeral:{job_id}

Status file: cache/mt5/terminals/{id}/status.json  (live mode only)
Port file:   cache/mt5/pids.json  (nested under "terminals" key)

Mini HTTP endpoints:
  GET  /ticks/{symbol}           → latest tick from memory
  GET  /ticks_batch              → latest tick for all symbols (single call)
  GET  /ticks_history/{symbol}   → historical ticks (history + ephemeral only)
  GET  /history/{symbol}/{tf}    → on-demand historic bars (history + ephemeral only)
  POST /sync_request             → no-op
  GET  /status                   → connection + tick status
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import concurrent.futures
import io
import json
import logging
import os
import signal
import socket
import sys
import threading
import time as _time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, StreamingResponse

# ---------------------------------------------------------------------------
# Bootstrap
# ---------------------------------------------------------------------------

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from shared.mt5_worker_config import DEFAULT_WORKER_CONFIG as DEFAULT_CONFIG
from live.market_connector import MT5MarketConnector
from shared.logging_config import suppress_httpx_info_logs
from shared.mt5_ownership import wait_for_startup_fence, worker_fence
from shared.mt5_startup import pinned_terminal, validate_source

logger = logging.getLogger("mt5_worker")

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

CACHE_DIR   = ROOT / "cache"
MT5_DIR     = CACHE_DIR / "mt5"
CONFIG_FILE = MT5_DIR / "config.json"
PIDS_FILE   = MT5_DIR / "pids.json"

# ---------------------------------------------------------------------------
# Shared state (set in _main_async before server starts)
# ---------------------------------------------------------------------------

_connector   = MT5MarketConnector()
_running     = True
_terminal_id = ""
_worker_mode = "live"   # "live" | "history" | "ephemeral"
_worker_job_id = ""     # ephemeral only

_status: dict[str, Any] = {
    "phase":       "idle",
    "broker":      "",
    "connected":   False,
    "ticks":       {},
    "last_update": None,
    "errors":      [],
}

# Broker UTC offset in hours — computed from US DST rules at startup
# and refreshed daily to track DST transitions.
# MT5 forex brokers use UTC+2 (winter/EST) or UTC+3 (summer/EDT) so that
# the NY 17:00 close always falls at broker midnight (00:00 server time).
_utc_offset_hours: int = 0

# Single-thread executor — MT5 Python lib is not thread-safe; all MT5 calls
# must go through this one thread.
_mt5_executor = concurrent.futures.ThreadPoolExecutor(
    max_workers=1, thread_name_prefix="mt5"
)

# Per-symbol live feed state
_last_bar_ts:          dict[str, int]   = {}   # symbol → last closed bar unix ts

# Periodic summary counters (written to stdout once per second by bar loop)
_ticks_sent   = 0
_bars_closed  = 0
_forming_sent = 0
_fetch_errors = 0

API_BASE             = "http://127.0.0.1:8000"

# ---------------------------------------------------------------------------
# History cache — O(1) LRU via OrderedDict, avoids redundant copy_rates_range()
# ---------------------------------------------------------------------------
# Key: (symbol, tf, from_iso, to_iso)  Value: (response_dict, cached_at, ttl_seconds)
from collections import OrderedDict

_history_cache: OrderedDict[tuple, tuple] = OrderedDict()
_history_cache_lock = threading.Lock()
_MAX_CACHE = 14

def _cache_ttl(to_dt: "datetime") -> float:
    """Historical ranges (to_date in the past) cache for 6 hours.
    Recent ranges (to_date = now) cache for 60 seconds."""
    age = (datetime.now(timezone.utc) - to_dt).total_seconds()
    return 21600.0 if age > 3600 else 60.0

def _get_cached(key: tuple) -> "dict | None":
    with _history_cache_lock:
        entry = _history_cache.get(key)
        if entry is None:
            return None
        resp, cached_at, ttl = entry
        if _time.monotonic() - cached_at > ttl:
            del _history_cache[key]
            return None
        _history_cache.move_to_end(key)
        return resp

def _set_cached(key: tuple, resp: dict, ttl: float) -> None:
    with _history_cache_lock:
        if key in _history_cache:
            _history_cache.move_to_end(key)
        elif len(_history_cache) >= _MAX_CACHE:
            _history_cache.popitem(last=False)  # evict oldest (LRU)
        _history_cache[key] = (resp, _time.monotonic(), ttl)


from shared.util import sanitize_source as _sanitize_source


def _detect_utc_offset(symbols: "list[str]") -> int:
    """Compute broker UTC offset from US DST rules.

    MT5 forex brokers align their server clock so that New York's 17:00 close
    falls at broker midnight (00:00 server time). This means:
      - US summer (EDT, UTC-4): NY 17:00 = 21:00 UTC = broker midnight → UTC+3
      - US winter (EST, UTC-5): NY 17:00 = 22:00 UTC = broker midnight → UTC+2

    US DST: starts 2nd Sunday of March, ends 1st Sunday of November.

    MT5 returns all timestamps in server time (not UTC), so D1 bars always
    open at 00:00 — we cannot detect the offset from bar timestamps alone.
    """
    from datetime import datetime, timezone

    now = datetime.now(timezone.utc)
    year = now.year

    # 2nd Sunday of March: find March 1, advance to first Sunday, then +7
    mar1_wd = datetime(year, 3, 1).weekday()  # 0=Mon … 6=Sun
    first_sun_mar = 1 + (6 - mar1_wd) % 7
    dst_start_day = first_sun_mar + 7  # 2nd Sunday
    dst_start = datetime(year, 3, dst_start_day, 2, 0, tzinfo=timezone.utc)  # 2am ET (approx)

    # 1st Sunday of November
    nov1_wd = datetime(year, 11, 1).weekday()
    first_sun_nov = 1 + (6 - nov1_wd) % 7
    dst_end = datetime(year, 11, first_sun_nov, 2, 0, tzinfo=timezone.utc)

    is_dst = dst_start <= now < dst_end
    offset = 3 if is_dst else 2

    logger.info(
        f"[{_terminal_id}] Broker UTC offset: UTC+{offset} "
        f"({'summer/EDT' if is_dst else 'winter/EST'} — "
        f"NY close at {'21' if is_dst else '22'}:00 UTC)"
    )
    return offset


# ---------------------------------------------------------------------------
# Status helpers
# ---------------------------------------------------------------------------


def _status_file() -> Path:
    return MT5_DIR / "terminals" / _terminal_id / "status.json"


def _write_status(**updates) -> None:
    _status.update(updates)
    _status["last_update"] = datetime.now(timezone.utc).isoformat()
    sf = _status_file()
    sf.parent.mkdir(parents=True, exist_ok=True)
    try:
        tmp = sf.with_suffix(".tmp")
        tmp.write_text(json.dumps(_status, indent=2, default=str), encoding="utf-8")
        tmp.replace(sf)
    except Exception:
        pass


# ---------------------------------------------------------------------------
# API notify helper
# ---------------------------------------------------------------------------


_api_client: "httpx.AsyncClient | None" = None

async def _get_api_client() -> "httpx.AsyncClient":
    global _api_client
    import httpx
    if _api_client is None or _api_client.is_closed:
        _api_client = httpx.AsyncClient(timeout=5.0)
    return _api_client

async def _post_to_api(path: str, payload: dict) -> None:
    try:
        client = await _get_api_client()
        r = await client.post(f"{API_BASE}{path}", json=payload)
        r.raise_for_status()
    except Exception as e:
        logger.debug(f"[{_terminal_id}] POST {path} failed: {e}")


# ---------------------------------------------------------------------------
# Mini HTTP server
# ---------------------------------------------------------------------------


mini_app = FastAPI(title="MT5 Terminal Worker", docs_url=None, redoc_url=None)


@mini_app.get("/ticks/{symbol}")
async def get_tick(symbol: str):
    ticks = _status.get("ticks", {})
    tick  = ticks.get(symbol.upper())
    if not tick:
        return JSONResponse({"error": "No tick data yet"}, status_code=404)
    return {"symbol": symbol.upper(), **tick}


@mini_app.get("/ticks_batch")
async def get_ticks_batch():
    """Return latest tick for all symbols in a single call.

    Used by live_broker_quality_agent for efficient polling (one request
    per broker instead of 28).
    """
    loop = asyncio.get_event_loop()

    def _fetch_all():
        import MetaTrader5 as mt5
        result = {}
        for sym in DEFAULT_CONFIG.symbols:
            try:
                tick = mt5.symbol_info_tick(sym)
                if tick is not None and tick.bid > 0 and tick.ask > 0:
                    result[sym] = {
                        "bid": float(tick.bid),
                        "ask": float(tick.ask),
                        "time_ms": int(tick.time_msc),
                    }
            except Exception:
                pass
        return result

    ticks = await loop.run_in_executor(_mt5_executor, _fetch_all)
    return {"ticks": ticks}


@mini_app.get("/ticks_history/{symbol}")
async def get_ticks_history(
    symbol: str,
    from_ms: int = 0,
    to_ms: int | None = None,
    max_count: int = 50_000,
):
    """Return historical ticks from MT5 (copy_ticks_from).

    Paginate by advancing from_ms to the last returned tick's time_msc + 1.
    Not available in live mode.
    """
    if _worker_mode == "live":
        return JSONResponse(
            {"error": "Tick history not served by live worker"},
            status_code=404,
        )

    loop = asyncio.get_event_loop()

    def _fetch():
        return _connector.get_ticks_since(symbol.upper(), from_ms, max_count)

    df = await loop.run_in_executor(_mt5_executor, _fetch)
    if df.empty:
        return {"symbol": symbol.upper(), "ticks": [], "count": 0}

    if to_ms is not None:
        df = df[df["time_msc"] <= to_ms]

    ticks = [
        {"time_msc": int(r.time_msc), "bid": float(r.bid), "ask": float(r.ask)}
        for r in df.itertuples(index=False)
    ]
    return {"symbol": symbol.upper(), "ticks": ticks, "count": len(ticks)}


# Timeframes MT5 doesn't support natively — must be resampled from base TF
_RESAMPLE_TF: dict[str, str] = {
    "MN3":  "QS",    # quarterly, anchored to Jan/Apr/Jul/Oct
    "MN6":  "6MS",   # semi-annual, anchored to Jan/Jul (6-month start)
    "MN12": "YS",    # annual, anchored to Jan
    "H9":   "9h",    # 9-hour bins, anchored to midnight
}

_RESAMPLE_BASE_TF: dict[str, str] = {
    "MN3":  "MN1",
    "MN6":  "MN1",
    "MN12": "MN1",
    "H9":   "H1",
}


def _resample_bars(df: "pd.DataFrame", rule: str) -> "pd.DataFrame":
    """Resample a base-TF DataFrame to a wider period (non-native TFs)."""
    import pandas as _pd
    df = df.copy()
    df["time"] = _pd.to_datetime(df["time"], utc=True)
    df = df.set_index("time").sort_index()
    resampled = df.resample(rule).agg(
        open=("open", "first"),
        high=("high", "max"),
        low=("low", "min"),
        close=("close", "last"),
        volume=("volume", "sum"),
    ).dropna(subset=["open"])
    resampled = resampled.reset_index()
    return resampled


@mini_app.get("/history/{symbol}/{tf}")
async def get_history(symbol: str, tf: str,
                      from_date: str = None, to_date: str = None,
                      months: int = 12):
    """On-demand historic bars from MT5.  Not available in live mode.

    If from_date and to_date provided (ISO 8601), use exact range.
    Otherwise use the last `months` months.

    MN3 / MN6 / MN12 are resampled from MN1 (not natively in MT5).

    Returns {"format": "parquet_b64", "data": "<base64>"} for large datasets
    (>5000 bars) or {"bars": [...], "symbol": ..., "tf": ...} for small ones.
    """
    if _worker_mode == "live":
        return JSONResponse({"error": "History not served by live worker — use history worker"}, status_code=404)

    t0 = _time.monotonic()
    logger.info(f"[{_terminal_id}][history] → /history/{symbol}/{tf} from={from_date} to={to_date} months={months}")

    loop = asyncio.get_event_loop()
    tz   = timezone.utc
    tf_upper = tf.upper()

    if from_date and to_date:
        fd = datetime.fromisoformat(from_date)
        td = datetime.fromisoformat(to_date)
        if fd.tzinfo is None:
            fd = fd.replace(tzinfo=tz)
        if td.tzinfo is None:
            td = td.replace(tzinfo=tz)
    else:
        td = datetime.now(tz)
        fd = td - timedelta(days=months * 30)

    # Round to_date to the nearest minute for cache key so recent queries
    # actually hit the cache (otherwise microsecond differences cause misses)
    td_rounded = td.replace(second=0, microsecond=0)
    cache_key = (symbol.upper(), tf_upper, fd.isoformat(), td_rounded.isoformat())
    cached = _get_cached(cache_key)
    if cached is not None:
        n_bars = cached.get("count", len(cached.get("bars", [])))
        logger.info(f"[{_terminal_id}][history] ← /history/{symbol}/{tf} {n_bars} bars dt={(_time.monotonic()-t0)*1000:.0f}ms cached")
        return cached

    # For synthetic TFs, fetch base TF and resample
    resample_rule = _RESAMPLE_TF.get(tf_upper)
    if resample_rule:
        base_tf = _RESAMPLE_BASE_TF.get(tf_upper, "MN1")
        def _fetch_base():
            return _connector.get_history(symbol.upper(), base_tf, fd, td, ensure_history=True)
        try:
            base_df = await asyncio.wait_for(loop.run_in_executor(_mt5_executor, _fetch_base), timeout=120.0)
        except asyncio.TimeoutError:
            logger.warning(f"[{_terminal_id}][history] ← /history/{symbol}/{tf} timed out after 120s")
            return JSONResponse({"error": "MT5 history request timed out"}, status_code=504)
        if base_df.empty:
            logger.info(f"[{_terminal_id}][history] ← /history/{symbol}/{tf} 0 bars dt={(_time.monotonic()-t0)*1000:.0f}ms")
            return {"bars": [], "symbol": symbol.upper(), "tf": tf_upper}
        df = _resample_bars(base_df, resample_rule)
    else:
        def _fetch():
            return _connector.get_history(symbol.upper(), tf_upper, fd, td, ensure_history=True)
        try:
            df = await asyncio.wait_for(loop.run_in_executor(_mt5_executor, _fetch), timeout=120.0)
        except asyncio.TimeoutError:
            logger.warning(f"[{_terminal_id}][history] ← /history/{symbol}/{tf} timed out after 120s")
            return JSONResponse({"error": "MT5 history request timed out"}, status_code=504)
        if df.empty:
            logger.info(f"[{_terminal_id}][history] ← /history/{symbol}/{tf} 0 bars dt={(_time.monotonic()-t0)*1000:.0f}ms")
            return {"bars": [], "symbol": symbol.upper(), "tf": tf_upper}

    df = df.copy()

    df["time"] = df["time"].dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    if len(df) > 5000:
        buf = io.BytesIO()
        df.to_parquet(buf, index=False, engine="pyarrow")
        result = {
            "format": "parquet_b64",
            "data":   base64.b64encode(buf.getvalue()).decode(),
            "symbol": symbol.upper(),
            "tf":     tf_upper,
            "count":  len(df),
        }
        _set_cached(cache_key, result, _cache_ttl(td))
        logger.info(f"[{_terminal_id}][history] ← /history/{symbol}/{tf} {len(df)} bars dt={(_time.monotonic()-t0)*1000:.0f}ms")
        return result

    result = {
        "symbol": symbol.upper(),
        "tf":     tf_upper,
        "bars":   df.to_dict(orient="records"),
    }
    _set_cached(cache_key, result, _cache_ttl(td))
    logger.info(f"[{_terminal_id}][history] ← /history/{symbol}/{tf} {len(df)} bars dt={(_time.monotonic()-t0)*1000:.0f}ms")
    return result


@mini_app.get("/status")
async def get_live_status():
    return _status


# ---------------------------------------------------------------------------
# Bar loop — closed bars + forming bar (100ms, MT5 authoritative)
# ---------------------------------------------------------------------------


async def _bar_loop(symbols: list[str], source: str) -> None:
    """Polls copy_rates_from_pos for all symbols in a single executor call.

    One MT5 thread invocation fetches all 28 symbols, then results are
    processed and broadcast. Only changed forming bars are sent.

    Emits:
      - live_bar  when the closed bar timestamp changes (new minute)
      - forming_bar when the forming bar OHLCV actually changed
    """
    global _ticks_sent, _bars_closed, _forming_sent, _fetch_errors
    loop = asyncio.get_event_loop()
    logger.info(f"[{_terminal_id}] Bar loop started — {len(symbols)} symbols")

    status_flush_counter = 0
    _last_summary = _time.monotonic()
    # Track last forming bar OHLCV to skip unchanged broadcasts
    _last_forming: dict[str, tuple] = {}  # symbol → (open, high, low, close, volume)

    def _fetch_all_bars() -> dict[str, tuple]:
        """Run on the MT5 thread — fetch latest 2 M1 bars for every symbol.
        Returns {symbol: (last_closed_row, forming_row)} as dicts."""
        results = {}
        for sym in symbols:
            try:
                df = _connector.get_latest_m1_bars(sym, count=2)
                if df is not None and len(df) >= 2:
                    closed = df.iloc[0]  # older — last completed bar
                    forming = df.iloc[1]  # newer — current incomplete bar
                    results[sym] = (
                        {
                            "time_iso": closed["time"].isoformat(),
                            "time_ts":  int(closed["time"].timestamp()),
                            "open":     float(closed["open"]),
                            "high":     float(closed["high"]),
                            "low":      float(closed["low"]),
                            "close":    float(closed["close"]),
                            "volume":   int(closed["volume"]),
                        },
                        {
                            "time_ts":  int(forming["time"].timestamp()),
                            "open":     float(forming["open"]),
                            "high":     float(forming["high"]),
                            "low":      float(forming["low"]),
                            "close":    float(forming["close"]),
                            "volume":   int(forming["volume"]),
                        },
                    )
            except Exception:
                pass
        return results

    while _running:
        await asyncio.sleep(0.1)
        now = loop.time()
        status_flush_counter += 1
        if _time.monotonic() - _last_summary >= 1.0:
            logger.info(f"[{_terminal_id}] ticks={_ticks_sent} bars_closed={_bars_closed} forming={_forming_sent} errors={_fetch_errors}")
            _ticks_sent = _bars_closed = _forming_sent = _fetch_errors = 0
            _last_summary = _time.monotonic()

        try:
            all_bars = await loop.run_in_executor(_mt5_executor, _fetch_all_bars)
        except Exception as e:
            _fetch_errors += 1
            logger.debug(f"[{_terminal_id}] bar_loop fetch error: {e}")
            continue

        for symbol, (closed, forming) in all_bars.items():
            # New closed bar — emit live_bar (live mode only)
            if _last_bar_ts.get(symbol, 0) != closed["time_ts"]:
                _last_bar_ts[symbol] = closed["time_ts"]
                _bars_closed += 1
                if _worker_mode == "live":
                    await _post_to_api("/internal/feed/notify", {
                        "type":   "live_bar",
                        "source": source,
                        "symbol": symbol,
                        "bar": {
                            "time":   closed["time_iso"],
                            "open":   closed["open"],
                            "high":   closed["high"],
                            "low":    closed["low"],
                            "close":  closed["close"],
                            "volume": closed["volume"],
                        },
                    })

            # Forming bar — only send if OHLCV actually changed (live mode only)
            forming_key = (forming["open"], forming["high"], forming["low"],
                           forming["close"], forming["volume"])
            if _last_forming.get(symbol) != forming_key:
                _last_forming[symbol] = forming_key
                _forming_sent += 1
                if _worker_mode == "live":
                    await _post_to_api("/internal/feed/notify", {
                        "type":         "forming_bar",
                        "source":       source,
                        "symbol":       symbol,
                        "bar_open_time": forming["time_ts"],
                        "open":          forming["open"],
                        "high":          forming["high"],
                        "low":           forming["low"],
                        "close":         forming["close"],
                        "volume":        forming["volume"],
                    })

        # Flush status to disk every ~3 s
        if status_flush_counter >= 30:
            status_flush_counter = 0
            ticks = _status.get("ticks", {})
            if ticks:
                tick_times = []
                for t in ticks.values():
                    try:
                        tick_times.append(datetime.fromisoformat(t["time"]))
                    except Exception:
                        pass
                if tick_times:
                    newest    = max(tick_times)
                    gap_hours = (datetime.now(timezone.utc) - newest).total_seconds() / 3600
                    _write_status(phase="weekend" if gap_hours > 2 else "live_feed")
                else:
                    _write_status()
            else:
                _write_status()

    logger.info(f"[{_terminal_id}] Bar loop stopped")


# ---------------------------------------------------------------------------
# Tick loop — real-time bid/ask with true millisecond timestamps (100ms)
# ---------------------------------------------------------------------------


async def _tick_loop(symbols: list[str], source: str) -> None:
    """Polls symbol_info_tick() for all symbols in a single executor call.

    Uses the fast symbol_info_tick() (returns MT5's cached latest tick) instead
    of copy_ticks_from() (queries tick history). One executor call fetches all
    28 symbols, then only changed ticks are broadcast — much faster than the
    previous sequential per-symbol approach.
    """
    global _ticks_sent, _fetch_errors
    loop = asyncio.get_event_loop()
    # Track last seen bid/ask per symbol to only broadcast changes
    _last_seen: dict[str, tuple[float, float]] = {}

    def _fetch_all_ticks() -> dict[str, dict]:
        """Run on the MT5 thread — fetch latest tick for every symbol."""
        import MetaTrader5 as mt5
        result = {}
        for sym in symbols:
            try:
                tick = mt5.symbol_info_tick(sym)
                if tick is not None and tick.bid > 0 and tick.ask > 0:
                    result[sym] = {
                        "bid": float(tick.bid),
                        "ask": float(tick.ask),
                        "time_ms": int(tick.time_msc),
                    }
            except Exception:
                pass
        return result

    logger.info(f"[{_terminal_id}] Tick loop started — {len(symbols)} symbols")

    while _running:
        await asyncio.sleep(0.1)

        try:
            all_ticks = await loop.run_in_executor(_mt5_executor, _fetch_all_ticks)
        except Exception as e:
            _fetch_errors += 1
            logger.debug(f"[{_terminal_id}] tick_loop fetch error: {e}")
            continue

        for symbol, tick in all_ticks.items():
            bid, ask = tick["bid"], tick["ask"]
            prev = _last_seen.get(symbol)
            if prev and prev[0] == bid and prev[1] == ask:
                continue  # no change — skip broadcast
            _last_seen[symbol] = (bid, ask)
            _ticks_sent += 1

            # Update proxy tick state (all modes)
            _status.setdefault("ticks", {})[symbol] = {
                "time": datetime.fromtimestamp(
                    tick["time_ms"] / 1000.0, tz=timezone.utc
                ).isoformat(),
                "bid": bid,
                "ask": ask,
            }

            # Only broadcast to API in live mode
            if _worker_mode == "live":
                await _post_to_api("/internal/feed/notify", {
                    "type":   "ticks",
                    "source": source,
                    "symbol": symbol,
                    "ticks":  [tick],
                })

    logger.info(f"[{_terminal_id}] Tick loop stopped")


# ---------------------------------------------------------------------------
# UTC offset refresh loop — re-detects once per day to track DST transitions
# ---------------------------------------------------------------------------


async def _offset_refresh_loop(symbols: list[str]) -> None:
    """Recompute broker UTC offset every 24 h to handle DST transitions.

    US DST transitions (2nd Sunday March / 1st Sunday November) shift the
    broker clock between UTC+2 and UTC+3.  Checking daily picks up the change.
    """
    global _utc_offset_hours

    while _running:
        await asyncio.sleep(86_400)   # 24 h
        if not _running:
            break
        try:
            new_offset = _detect_utc_offset(symbols)
            if new_offset != _utc_offset_hours:
                logger.info(
                    f"[{_terminal_id}] DST transition detected: "
                    f"UTC+{_utc_offset_hours} → UTC+{new_offset} — clearing history cache"
                )
                _utc_offset_hours = new_offset
                with _history_cache_lock:
                    _history_cache.clear()
                _write_status(utc_offset=_utc_offset_hours)
        except Exception as e:
            logger.debug(f"[{_terminal_id}] offset_refresh_loop error: {e}")


# ---------------------------------------------------------------------------
# Port + PID helpers
# ---------------------------------------------------------------------------


def _find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _write_pid_port(port: int, mql5_source: str = "") -> None:
    """Write worker port into pids.json using the new nested format.

    New format:
      terminals[terminal_id]["live"]     = {pid, port, mql5_source}
      terminals[terminal_id]["history"]  = {pid, port, mql5_source}
      terminals[terminal_id]["ephemeral"] = [{pid, port, mql5_source, job_id}, ...]

    Uses atomic temp-file rename to prevent corruption from concurrent writers.
    Retries on read failure in case another writer is mid-rename.
    """
    import time as _time
    PIDS_FILE.parent.mkdir(parents=True, exist_ok=True)

    pid_info = {
        "pid":         os.getpid(),
        "port":        port,
        "mql5_source": mql5_source,
        "started_at":  _time.time(),
    }

    for _attempt in range(10):
        data: dict = {}
        if PIDS_FILE.exists():
            try:
                raw = PIDS_FILE.read_text(encoding="utf-8").strip()
                if raw:
                    data = json.loads(raw)
                else:
                    logger.warning(f"pids.json is empty — starting fresh")
            except Exception:
                logger.warning(f"pids.json is corrupt — starting fresh")

        data.setdefault("terminals", {})
        entry = data["terminals"].setdefault(_terminal_id, {})

        if _worker_mode == "ephemeral":
            ephemerals = entry.setdefault("ephemeral", [])
            # Replace stale entry for this job_id if present
            entry["ephemeral"] = [e for e in ephemerals if e.get("job_id") != _worker_job_id]
            entry["ephemeral"].append({**pid_info, "job_id": _worker_job_id})
        else:
            entry[_worker_mode] = pid_info

        try:
            tmp = PIDS_FILE.with_suffix(".tmp")
            tmp.write_text(json.dumps(data, indent=2), encoding="utf-8")
            tmp.replace(PIDS_FILE)
            return
        except Exception:
            _time.sleep(0.1)


async def _register_with_api(port: int, mql5_source: str, retries: int = 10, *, stop_on_failure=True) -> None:
    import httpx
    import time as _time

    if _worker_mode == "ephemeral":
        worker_id   = f"mt5_ephemeral:{_worker_job_id}:{_terminal_id}"
        worker_type = "mt5_ephemeral"
        job_id      = _worker_job_id
    else:
        worker_id   = f"mt5_{_worker_mode}:{_terminal_id}"
        worker_type = f"mt5_{_worker_mode}"
        job_id      = None

    payload = {
        "worker_id":   worker_id,
        "pid":         os.getpid(),
        "port":        port,
        "mql5_source": mql5_source,
        "worker_type": worker_type,
        "job_id":      job_id,
        "started_at":  _time.time(),
        "terminal_id": _terminal_id,
    }
    for attempt in range(retries):
        try:
            async with httpx.AsyncClient(timeout=3.0) as client:
                r = await client.post(
                    f"{API_BASE}/internal/worker/register", json=payload
                )
                if r.status_code == 200:
                    logger.info(
                        f"[{_terminal_id}] Registered as {worker_id!r} "
                        f"(pid={os.getpid()}, port={port}, source={mql5_source})"
                    )
                    return
        except Exception as e:
            logger.debug(f"[{_terminal_id}] Registry attempt {attempt+1}/{retries} failed: {e}")
        await asyncio.sleep(2)
    if stop_on_failure:
        logger.error(f"[{_terminal_id}] Could not register with API after {retries} attempts — shutting down")
        global _running
        _running = False


async def _registration_loop(port, source):
    """Reconnect surviving workers to a replaced registry without launching duplicates."""
    while _running:
        await asyncio.sleep(10)
        if _running:
            await _register_with_api(port, source, retries=1, stop_on_failure=False)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def _initialize_runtime_terminal(terminal_path, terminal_id, activation):
    """Serialize native connection/possible terminal launch without blocking explicit Stop."""
    import time
    from api.runtime_session import assert_local_launch
    from shared.mt5_ownership import FileFence, OwnershipBusy
    deadline = time.monotonic() + 25
    while True:
        try:
            fence = FileFence(ROOT / 'cache/mt5/ownership/terminal-connect.lock').acquire()
            break
        except OwnershipBusy:
            if time.monotonic() >= deadline:
                raise ValueError('Terminal connection is busy')
            time.sleep(0.05)
    try:
        assert_local_launch(ROOT, terminal_id, activation)
        import psutil
        from shared.windows_session import process_session
        existing = [p for p in psutil.process_iter(['exe']) if p.info['exe'] and
                    Path(p.info['exe']).resolve() == Path(terminal_path).resolve() and
                    process_session(p.pid) == process_session(os.getpid())]
        if len(existing) > 1:
            raise ValueError('Ambiguous MT5 instances in the runtime desktop')
        if existing and existing[0].username().casefold() != psutil.Process().username().casefold():
            raise ValueError('MT5 is owned by another account')
        if not _connector.initialize(terminal_path):
            return False
        info = _connector.get_terminal_info()
        if (not info or Path(info['path']).resolve() != Path(terminal_path).resolve().parent or
                not Path(info['data_path']).resolve().is_relative_to(Path.home().resolve())):
            _connector.shutdown()
            raise ValueError('MT5 executable/profile does not belong to this runtime login')
        return True
    finally:
        fence.release()


async def _main_async(terminal_id: str, mode: str = "live", job_id: str = "", *,
                      term_cfg: dict | None = None, startup_fence=None,
                      expected_source: str | None = None, runtime_activation=None) -> None:
    global _running, _terminal_id, _utc_offset_hours, _worker_mode, _worker_job_id  # noqa: PLW0603
    _terminal_id   = terminal_id
    _worker_mode   = mode
    _worker_job_id = job_id

    # Live/history must receive the snapshot validated under the shared fence.
    # Agent-private ephemeral workers retain their existing separate lifecycle.
    if mode == "ephemeral":
        cfg = json.loads(CONFIG_FILE.read_text(encoding="utf-8"))
        term_cfg = next((t for t in cfg.get("terminals", []) if t["id"] == terminal_id), None)
    elif term_cfg is None or startup_fence is None:
        raise ValueError("MT5 live/history startup requires an ownership pin")

    if not term_cfg or not term_cfg.get("terminal_path"):
        _write_status(phase="error",
                      errors=[f"Terminal ID {terminal_id!r} not found or no terminal_path"])
        return

    terminal_path = term_cfg["terminal_path"]
    symbols       = DEFAULT_CONFIG.symbols

    # Initialize status — preserve broker/mql5_source from previous run so the
    # broker dropdown doesn't flash the terminal ID while we reconnect.
    prev_status = {}
    sf = _status_file()
    if sf.exists():
        try:
            prev_status = json.loads(sf.read_text(encoding="utf-8"))
        except Exception:
            pass
    _status.update({
        "id":            terminal_id,
        "label":         term_cfg.get("label", term_cfg.get("name", terminal_id)),
        "terminal_path": terminal_path,
        "active":        term_cfg.get("active", True),
        "broker":        prev_status.get("broker", ""),
        "mql5_source":   prev_status.get("mql5_source", ""),
    })

    # Connect (only the live worker writes to the shared status file)
    if mode == "live":
        _write_status(phase="connecting", connected=False)
    loop = asyncio.get_event_loop()

    # The immutable snapshot is now captured and this process still owns its
    # lifetime/mode fence. Do not hold the *operation* fence over native MT5 calls:
    # a hung initializer must remain killable by an exact-identity private stop.
    # Pending children remain visible even before PID/registry publication.
    if startup_fence is not None:
        startup_fence.release()

    try:
        ok = await asyncio.wait_for(
            loop.run_in_executor(None, lambda: _initialize_runtime_terminal(terminal_path, terminal_id, runtime_activation)
                if mode != 'ephemeral' else _connector.initialize(terminal_path)),
            timeout=30.0,
        )
    except asyncio.TimeoutError:
        if mode == "live":
            _write_status(phase="error", errors=["MT5 initialize timed out (30s)"])
        logger.error(f"[{terminal_id}][{mode}] MT5 initialize timed out — is the terminal running?")
        return
    if not ok:
        if mode == "live":
            _write_status(phase="error", errors=["MT5 initialize failed"])
        logger.error(f"[{terminal_id}][{mode}] MT5 initialize failed")
        return

    info        = await loop.run_in_executor(None, _connector.get_terminal_info)
    broker      = info.get("broker", "Unknown") if info else "Unknown"
    mql5_source = _sanitize_source(broker)
    validate_source(mql5_source, expected_source)

    # Compute broker UTC offset from US DST rules (UTC+3 summer, UTC+2 winter)
    _utc_offset_hours = _detect_utc_offset(symbols)

    mode_tag = f"[{mode}]" if mode != "live" else ""
    if mode == "live":
        _write_status(phase="live_feed", connected=True, broker=broker,
                      mql5_source=mql5_source, utc_offset=_utc_offset_hours)
    logger.info(f"[{terminal_id}]{mode_tag} Connected — broker: {broker}, GMT+{_utc_offset_hours}")

    # Start mini HTTP server
    port = _find_free_port()
    _write_pid_port(port, mql5_source)

    server_config = uvicorn.Config(
        mini_app,
        host="127.0.0.1",
        port=port,
        log_level="warning",
        loop="asyncio",
    )
    server = uvicorn.Server(server_config)

    def _on_sigterm(*_):
        global _running
        _running = False
        server.should_exit = True
        logger.info(f"[{terminal_id}]{mode_tag} SIGTERM — shutting down")

    signal.signal(signal.SIGTERM, _on_sigterm)
    signal.signal(signal.SIGINT,  _on_sigterm)

    asyncio.create_task(_register_with_api(port, mql5_source))

    # Build task list based on mode
    # history:   server only (no loops, no broadcasting)
    # live:      server + bar + tick + offset refresh
    # ephemeral: server + bar + tick (no broadcast, no offset refresh needed)
    tasks = [server.serve()]
    if mode in ("live", "ephemeral"):
        tasks.append(_bar_loop(symbols, mql5_source))
        tasks.append(_tick_loop(symbols, mql5_source))
    if mode == "live":
        tasks.append(_offset_refresh_loop(symbols))

    tasks.append(_registration_loop(port, mql5_source))
    await asyncio.gather(*tasks)

    _connector.shutdown()
    if mode == "live":
        _write_status(phase="stopped", connected=False)
    logger.info(f"[{terminal_id}]{mode_tag} Worker stopped")


def main():
    parser = argparse.ArgumentParser(description="Unified MT5 terminal worker")
    parser.add_argument("--terminal-id", required=True, help="Terminal ID from config")
    parser.add_argument("--config-revision", help="Required immutable live/history configuration pin")
    parser.add_argument('--runtime-activation', help='Required approved Windows login activation')
    parser.add_argument("--expected-source", help="Optional approved exact source identity")
    parser.add_argument('--symbols', nargs='+', help='Trusted host-selected market symbols')
    parser.add_argument(
        "--mode", default="live", choices=["live", "history", "ephemeral"],
        help="Worker mode: live (ticks+bars+broadcast), history (chart data only), ephemeral (agent-private)",
    )
    parser.add_argument(
        "--job-id", default="", help="Job ID (required for ephemeral mode)",
    )
    args = parser.parse_args()
    if args.symbols is not None:
        if any(not symbol.replace('_', '').isalnum() for symbol in args.symbols):
            parser.error('Invalid market symbol')
        DEFAULT_CONFIG.symbols = list(dict.fromkeys(args.symbols))

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
    )
    suppress_httpx_info_logs()
    if args.mode == "ephemeral":
        asyncio.run(_main_async(args.terminal_id, mode=args.mode, job_id=args.job_id))
        return
    if not args.config_revision:
        parser.error("live/history workers require --config-revision from the owned launcher")
    if not args.runtime_activation:
        parser.error('live/history workers require an approved runtime login')
    # Lifetime lock precedes any status writes, connector initialization or bind.
    # Contention fails closed: the private host may retry after startup completes.
    with worker_fence(ROOT, args.terminal_id, args.mode):
        startup_fence = wait_for_startup_fence(ROOT)
        try:
            term_cfg = pinned_terminal(CONFIG_FILE, args.terminal_id, args.config_revision)
            from api.runtime_session import assert_local_launch
            assert_local_launch(ROOT, args.terminal_id, args.runtime_activation)
            try:
                asyncio.run(_main_async(args.terminal_id, mode=args.mode, job_id=args.job_id,
                    term_cfg=term_cfg, startup_fence=startup_fence, expected_source=args.expected_source,
                    runtime_activation=args.runtime_activation))
            finally:
                # asyncio.run drains initializer executor work before lock release,
                # even after timeout/cancellation. Do not abandon a late connector.
                _connector.shutdown()
        finally:
            startup_fence.release()


if __name__ == "__main__":
    main()
