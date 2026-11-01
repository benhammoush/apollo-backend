"""
live/market_connector.py
=====================
Market-only MT5 terminal connection wrapper.

Wraps the MetaTrader5 Python library to:
  - Connect to a specific terminal
  - Download historical M1 bars (same column structure as Massive: time, open, high, low, close, volume)
  - Poll latest ticks

MetaTrader5 limitation: one connection per process.
Each terminal runs as its own subprocess (run_mt5_worker.py).

Install: pip install MetaTrader5
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import List, Optional

import pandas as pd

logger = logging.getLogger('live.mt5_connector')

# ---------------------------------------------------------------------------
# MT5 timeframe map
# ---------------------------------------------------------------------------

_TF_CONST_MAP: dict[str, int] = {
    "M1":  1,
    "M5":  5,
    "M15": 15,
    "H1":  16385,
    "H4":  16388,
    "D1":  16408,
    "W1":  32769,
    "MN1": 49153,
}


def _tf_const(tf: str) -> int:
    """Return the MT5 TIMEFRAME_* integer constant for a TF string."""
    try:
        import MetaTrader5 as mt5
        _map = {
            "M1":  mt5.TIMEFRAME_M1,
            "M5":  mt5.TIMEFRAME_M5,
            "M15": mt5.TIMEFRAME_M15,
            "H1":  mt5.TIMEFRAME_H1,
            "H4":  mt5.TIMEFRAME_H4,
            "D1":  mt5.TIMEFRAME_D1,
            "W1":  mt5.TIMEFRAME_W1,
            "MN1": mt5.TIMEFRAME_MN1,
        }
        return _map.get(tf, mt5.TIMEFRAME_M1)
    except ImportError:
        return _TF_CONST_MAP.get(tf, 1)


# ---------------------------------------------------------------------------
# MT5MarketConnector
# ---------------------------------------------------------------------------


class MT5MarketConnector:
    """
    Single-terminal MT5 connection wrapper.

    One instance per process — MetaTrader5 library supports only one
    terminal connection per process.

    Usage:
        conn = MT5MarketConnector()
        ok = conn.initialize("C:/Program Files/MetaTrader 5/terminal64.exe")
        info = conn.get_terminal_info()
        df = conn.get_history_m1("EURUSD", from_date, to_date)
        conn.shutdown()
    """

    def __init__(self) -> None:
        self._initialized = False

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def initialize(self, terminal_path: str) -> bool:
        """
        Connect to the MT5 terminal at the given exe path.
        Launches the terminal if not already running.
        Returns True on success.
        """
        try:
            import MetaTrader5 as mt5
        except ImportError:
            logger.error(
                "MetaTrader5 package not installed — run: pip install MetaTrader5"
            )
            return False

        if not mt5.initialize(terminal_path):
            logger.error(f"MT5 initialize failed: {mt5.last_error()}")
            return False

        self._initialized = True
        logger.info(f"MT5 connected: {terminal_path}")
        return True

    def shutdown(self) -> None:
        """Disconnect from MT5 terminal."""
        if self._initialized:
            try:
                import MetaTrader5 as mt5
                mt5.shutdown()
            except Exception:
                pass
            self._initialized = False
            logger.info("MT5 connection closed")

    @property
    def connected(self) -> bool:
        """``True`` if the terminal connection has been successfully initialized."""
        return self._initialized

    # ------------------------------------------------------------------
    # Terminal info
    # ------------------------------------------------------------------

    def get_terminal_info(self) -> Optional[dict]:
        """Return broker/version info for the connected terminal."""
        if not self._initialized:
            return None
        try:
            import MetaTrader5 as mt5
            info    = mt5.terminal_info()
            account = mt5.account_info()
            if info is None:
                return None
            return {
                "broker":    getattr(account, "company", "Unknown") if account else "Unknown",
                "server":    getattr(account, "server",  "")         if account else "",
                "version":   str(info.build),
                "path":      info.path,
                "data_path": info.data_path,
            }
        except Exception as e:
            logger.warning(f"get_terminal_info error: {e}")
            return None

    # ------------------------------------------------------------------
    # Historical data
    # ------------------------------------------------------------------

    def _ensure_symbol_loaded(
        self, symbol: str, from_date: datetime, to_date: datetime, timeout_s: int = 60,
        *, tf: str = "M1",
    ) -> None:
        """
        Wait for available history in the requested timeframe (M1 by default).

        MT5 downloads broker history asynchronously.  The first call to
        copy_rates_range() triggers the download and returns immediately with
        empty or partial data; subsequent calls return more bars as the
        terminal receives them from the broker.

        Strategy:
          1. Enable the symbol in Market Watch (symbol_select)
           2. Call copy_rates_range() for that timeframe to trigger the download
          3. Retry every second until the newest bar returned is within 1 hour
              of to_date, or available history stabilizes, or 60 s elapse.
              This does not guarantee that the entire requested range is available.
          4. Log the last MT5 error on timeout so the caller can diagnose issues
        """
        import time
        import MetaTrader5 as mt5

        mt5.symbol_select(symbol, True)
        tf_val = _tf_const(tf)

        prev_last_ts = None
        stable_count = 0

        for attempt in range(timeout_s):
            rates = mt5.copy_rates_range(symbol, tf_val, from_date, to_date)
            if rates is None:
                error = mt5.last_error()
                if error[0] == mt5.RES_E_INVALID_PARAMS:
                    logger.warning(f"[{symbol}][{tf}] MT5 history request rejected: {error}")
                    return
            if rates is not None and len(rates) > 0:
                last_ts = datetime.fromtimestamp(rates[-1]["time"], tz=timezone.utc)

                # Accept immediately when data reaches within 1 hour of to_date.
                if (to_date - last_ts).total_seconds() < 3600:
                    logger.debug(
                        f"[{symbol}] History ready: {len(rates):,} bars, "
                        f"last={last_ts.date()}"
                    )
                    return

                # Accept when the last bar has not changed across two consecutive
                # calls — the broker has no newer data (market closed / weekend /
                # holiday).  This avoids a 60-second wait every weekend.
                if last_ts == prev_last_ts:
                    stable_count += 1
                    if stable_count >= 2:
                        logger.debug(
                            f"[{symbol}] History stable at {last_ts.date()} "
                            f"— broker has no newer data (market may be closed)"
                        )
                        return
                else:
                    stable_count = 0
                    prev_last_ts = last_ts

            time.sleep(1)
            if attempt % 10 == 9:
                logger.debug(
                    f"[{symbol}] Still waiting for MT5 history "
                    f"(attempt {attempt + 1}/60, last_error={mt5.last_error()})"
                )

        logger.warning(
            f"[{symbol}] MT5 history not fully loaded after 60 s "
            f"— proceeding with available data. last_error={mt5.last_error()}"
        )

    def get_history_m1(
        self,
        symbol: str,
        from_date: datetime,
        to_date: datetime,
    ) -> pd.DataFrame:
        """
        Download M1 bars from the MT5 terminal.

        Returns DataFrame with columns:
            time (UTC-aware datetime64), open, high, low, close, volume

        This is the exact same column structure as the Massive downloader,
        so ohlc_cache.build_symbol_dataset() works without any changes.
        """
        if not self._initialized:
            return pd.DataFrame()
        try:
            import MetaTrader5 as mt5

            self._ensure_symbol_loaded(symbol, from_date, to_date)

            rates = mt5.copy_rates_range(
                symbol, mt5.TIMEFRAME_M1, from_date, to_date
            )
            if rates is None or len(rates) == 0:
                logger.warning(f"No M1 data for {symbol}: {mt5.last_error()}")
                return pd.DataFrame()

            df = pd.DataFrame(rates)[
                ["time", "open", "high", "low", "close", "tick_volume"]
            ]
            df.rename(columns={"tick_volume": "volume"}, inplace=True)
            df["time"] = pd.to_datetime(df["time"], unit="s", utc=True)
            return df.sort_values("time").reset_index(drop=True)
        except Exception as e:
            logger.warning(f"get_history_m1 error for {symbol}: {e}")
            return pd.DataFrame()

    def get_history(
        self,
        symbol: str,
        tf: str,
        from_date: datetime,
        to_date: datetime,
        ensure_history: bool = False,
    ) -> pd.DataFrame:
        """
        Download any supported timeframe from the terminal.
        Used by secondary workers for on-demand historic reads.

        copy_rates_range is known to miss the most recent ~3 hours of bars.
        When to_date is close to now, we supplement with copy_rates_from_pos
        which always has the latest data.

        Set ensure_history=True to wait for MT5 to finish downloading broker
        history before calling copy_rates_range.  This avoids the empty-response
        retry storm that can occur for symbols whose history has not been
        previously cached by the terminal.
        """
        if not self._initialized:
            return pd.DataFrame()
        try:
            import MetaTrader5 as mt5
            mt5.symbol_select(symbol, True)
            if ensure_history:
                self._ensure_symbol_loaded(symbol, from_date, to_date, tf=tf)
            tf_val = _tf_const(tf)
            rates  = mt5.copy_rates_range(symbol, tf_val, from_date, to_date)

            cols = ["time", "open", "high", "low", "close", "tick_volume"]

            # Supplement with copy_rates_from_pos for recent bars when
            # to_date is near now (i.e. the caller wants up-to-date data).
            # Window is 4 hours (14400s) to cover copy_rates_range's known
            # ~3-hour gap even under queuing delays from the single-threaded
            # MT5 executor, _ensure_symbol_loaded blocking, and API semaphore.
            now = datetime.now(timezone.utc)
            if (now - to_date).total_seconds() < 14400:
                recent = mt5.copy_rates_from_pos(symbol, tf_val, 0, 250)
                if recent is not None and len(recent) > 0:
                    if rates is not None and len(rates) > 0:
                        import numpy as np
                        # Merge: keep all from rates, add any from recent
                        # whose time is not already present
                        existing_times = set(rates["time"])
                        mask = [t not in existing_times for t in recent["time"]]
                        new_bars = recent[mask]
                        if len(new_bars) > 0:
                            rates = np.concatenate([rates, new_bars])
                    else:
                        rates = recent

            if rates is None or len(rates) == 0:
                return pd.DataFrame()

            df = pd.DataFrame(rates)[cols]
            df.rename(columns={"tick_volume": "volume"}, inplace=True)
            df["time"] = pd.to_datetime(df["time"], unit="s", utc=True)
            # Only filter by from_date; copy_rates_range already bounds the upper
            # limit, and supplement bars added via copy_rates_from_pos may have
            # server-time timestamps that appear to exceed a UTC to_date.
            df = df[df["time"] >= pd.Timestamp(from_date)]
            return df.sort_values("time").reset_index(drop=True)
        except Exception as e:
            logger.warning(f"get_history error for {symbol}/{tf}: {e}")
            return pd.DataFrame()

    def get_latest_m1_bars(self, symbol: str, count: int = 2) -> pd.DataFrame:
        """
        Get the last `count` completed M1 bars.
        Used by the live feed loop to detect new bars.
        """
        if not self._initialized:
            return pd.DataFrame()
        try:
            import MetaTrader5 as mt5
            rates = mt5.copy_rates_from_pos(
                symbol, mt5.TIMEFRAME_M1, 0, count
            )
            if rates is None or len(rates) == 0:
                return pd.DataFrame()

            df = pd.DataFrame(rates)[
                ["time", "open", "high", "low", "close", "tick_volume"]
            ]
            df.rename(columns={"tick_volume": "volume"}, inplace=True)
            df["time"] = pd.to_datetime(df["time"], unit="s", utc=True)
            return df.sort_values("time").reset_index(drop=True)
        except Exception as e:
            logger.debug(f"get_latest_m1_bars error for {symbol}: {e}")
            return pd.DataFrame()

    # ------------------------------------------------------------------
    # Live ticks
    # ------------------------------------------------------------------

    def get_last_tick(self, symbol: str) -> Optional[dict]:
        """Get the latest tick for a symbol. Returns {time, bid, ask} or None."""
        if not self._initialized:
            return None
        try:
            import MetaTrader5 as mt5
            tick = mt5.symbol_info_tick(symbol)
            if tick is None:
                return None
            return {
                "time": datetime.fromtimestamp(
                    tick.time, tz=timezone.utc
                ).isoformat(),
                "bid": float(tick.bid),
                "ask": float(tick.ask),
            }
        except Exception as e:
            logger.debug(f"get_last_tick error for {symbol}: {e}")
            return None

    def get_ticks_since(self, symbol: str, from_ms: int, max_count: int = 50_000) -> pd.DataFrame:
        """All ticks with time_msc > from_ms.
        Returns DataFrame with columns: time_msc (int64 UTC ms), bid, ask.
        Filters bid>0 and ask>0 to exclude non-quote (volume/spread) ticks."""
        if not self._initialized:
            return pd.DataFrame(columns=["time_msc", "bid", "ask"])
        try:
            import MetaTrader5 as mt5
            from_dt = datetime.fromtimestamp(from_ms / 1000.0, tz=timezone.utc)
            ticks = mt5.copy_ticks_from(symbol, from_dt, max_count, mt5.COPY_TICKS_ALL)
            if ticks is None or len(ticks) == 0:
                return pd.DataFrame(columns=["time_msc", "bid", "ask"])
            df = pd.DataFrame(ticks)[["time_msc", "bid", "ask"]]
            df = df[(df["time_msc"] > from_ms) & (df["bid"] > 0) & (df["ask"] > 0)]
            return df.reset_index(drop=True)
        except Exception as e:
            logger.debug(f"get_ticks_since error for {symbol}: {e}")
            return pd.DataFrame(columns=["time_msc", "bid", "ask"])
