"""Offline market-worker capability checks, suitable for the public candidate."""

import unittest
from unittest.mock import patch


class PublicMarketWorkerTests(unittest.TestCase):
    """Construct no terminal, listener or native client; inspect only the app graph."""

    def test_market_worker_has_no_account_trade_routes_or_connector_methods(self):
        with patch.dict('sys.modules', {'MetaTrader5': None}):
            from live import run_mt5_worker as worker
            from live.market_connector import MT5MarketConnector
            paths = {route.path for route in worker.mini_app.routes}
            self.assertTrue({'/history/{symbol}/{tf}', '/ticks/{symbol}', '/status'} <= paths)
            self.assertTrue({'/positions', '/trades/history', '/order', '/ohlc/export'}.isdisjoint(paths))
            for name in ('get_positions', 'get_trade_history', 'send_order'):
                self.assertFalse(hasattr(MT5MarketConnector, name))
            self.assertIs(type(worker._connector), MT5MarketConnector)
