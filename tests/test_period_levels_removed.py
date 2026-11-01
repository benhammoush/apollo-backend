"""Guard the removal of computed Levels without removing selected result reads."""

import importlib.util
import unittest
from unittest.mock import AsyncMock, patch

import httpx

from api.read_only_fixture import FixtureServices, fixture_application
from api.read_only_live import LiveReadServices


class PeriodLevelsRemovalTests(unittest.IsolatedAsyncioTestCase):
    """No computed Levels route, adapter or implementation belongs in this edition."""

    def test_computation_and_adapters_are_absent(self) -> None:
        """Deleting only the file must not leave import-time or deferred dangling callers."""
        self.assertIsNone(importlib.util.find_spec("shared.period_levels"))
        self.assertFalse(hasattr(LiveReadServices, "levels"))
        self.assertFalse(hasattr(FixtureServices, "levels"))

    async def test_removed_route_is_denied_without_market_work(self) -> None:
        """Direct and API-prefixed requests cannot trigger computation or consume quota."""
        origin = "http://127.0.0.1:5181"
        with fixture_application((origin,)) as app:
            self.assertNotIn("/levels/{symbol}", {route.path for route in app.http.routes})
            with patch.object(app.services, "market", new=AsyncMock()) as market:
                async with httpx.AsyncClient(
                    transport=httpx.ASGITransport(app=app),
                    base_url=origin,
                ) as client:
                    for path in ("/levels/EURUSD", "/api/levels/EURUSD", "/api/levels/UNKNOWN"):
                        response = await client.get(path)
                        self.assertEqual(response.status_code, 404)
                        response = await client.options(
                            path,
                            headers={
                                "Origin": origin,
                                "Access-Control-Request-Method": "GET",
                            },
                        )
                        self.assertEqual(response.status_code, 403)
                    self.assertEqual(app._active, {"metadata": 0, "work": 0})
                    self.assertFalse(app._arrivals["work"])
                    market.assert_not_awaited()

    async def test_selected_levels_result_remains_a_read_not_a_calculation(self) -> None:
        """Removing computation does not erase the independently selected dataset contract."""
        origin = "http://127.0.0.1:5181"
        with fixture_application((origin,)) as app:
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url=origin,
            ) as client:
                response = await client.get("/public/results/demo-levels")
                self.assertEqual(response.status_code, 200)
                self.assertEqual(response.json()["total"], 1)
