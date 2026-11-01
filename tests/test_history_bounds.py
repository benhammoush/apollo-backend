"""Keep supplemented MT5 history inside the requested inclusive UTC interval."""

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

import numpy as np

from live.market_connector import MT5MarketConnector


class HistoryBoundsTests(unittest.TestCase):
    """Use synthetic rates and a fake module, never a native terminal connection."""

    def test_recent_supplement_respects_both_bounds_and_preserves_range_rows(self) -> None:
        """Merge and fallback paths keep inclusive edges, deduplication and UTC semantics."""
        start = datetime(2026, 1, 2, 12, tzinfo=timezone.utc)
        end = start + timedelta(minutes=2)
        dtype = [
            ("time", "i8"),
            ("open", "f8"),
            ("high", "f8"),
            ("low", "f8"),
            ("close", "f8"),
            ("tick_volume", "i8"),
        ]
        recent = np.array(
            [
                (int((start + timedelta(minutes=minute)).timestamp()), 2.0, 3.0, 1.0, 2.5, 20)
                for minute in (3, 2, 1, 0, -1)
            ],
            dtype=dtype,
        )
        original = np.array([(int(start.timestamp()), 1.0, 2.0, 0.5, 1.5, 10)], dtype=dtype)
        clock = Mock()
        clock.now.return_value = end + timedelta(minutes=1)
        for rates in (original, None, np.array([], dtype=dtype)):
            for zone in (timezone.utc, timezone(timedelta(hours=2))):
                with self.subTest(has_range=rates is original, zone=zone):
                    native = SimpleNamespace(
                        symbol_select=Mock(),
                        copy_rates_range=Mock(return_value=rates),
                        copy_rates_from_pos=Mock(return_value=recent),
                    )
                    connector = MT5MarketConnector()
                    connector._initialized = True
                    with (
                        patch.dict("sys.modules", {"MetaTrader5": native}),
                        patch("live.market_connector._tf_const", return_value=1),
                        patch("live.market_connector.datetime", clock),
                    ):
                        result = connector.get_history(
                            "EURUSD", "M1", start.astimezone(zone), end.astimezone(zone)
                        )
                    self.assertEqual(
                        result["time"].dt.strftime("%H:%M").tolist(), ["12:00", "12:01", "12:02"]
                    )
                    self.assertEqual(
                        result["open"].tolist(), [1.0 if rates is original else 2.0, 2.0, 2.0]
                    )
                    self.assertEqual(
                        result["volume"].tolist(), [10 if rates is original else 20, 20, 20]
                    )
                    native.copy_rates_from_pos.assert_called_once()
