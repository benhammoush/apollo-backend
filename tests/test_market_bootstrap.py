"""The visitor API never grants browser clients worker lifecycle control."""

from dataclasses import replace
import unittest

import httpx

from api.market_bootstrap import MarketFeedBootstrap
from api.read_only import ReadOnlyApplication
from api.read_only_fixture import fixture_application

ORIGIN = "http://127.0.0.1:5181"


class VisitorWorkerControlTests(unittest.IsolatedAsyncioTestCase):
    """Only Windows-local operator controls may start the shared workers."""

    async def asyncSetUp(self):
        self.fixture = fixture_application((ORIGIN,))
        self.base = self.fixture.__enter__()
        self.client = httpx.AsyncClient(transport=httpx.ASGITransport(app=self.base), base_url=ORIGIN)

    async def asyncTearDown(self):
        await self.client.aclose()
        self.fixture.__exit__(None, None, None)

    async def test_visitor_capabilities_and_worker_status_are_always_read_only(self):
        self.assertEqual((await self.client.get("/capabilities")).json(),
                         {"read_only": True, "fixture": True})
        self.assertEqual((await self.client.get("/health")).json()["read_only"], True)
        self.assertEqual((await self.client.get("/public/market-feed")).json(),
                         {"enabled": False, "state": "disabled", "can_start": False,
                          "retry_after_seconds": 0})

    async def test_worker_start_is_denied_even_for_an_allowed_origin_and_payload(self):
        for options in ({"headers": {"Origin": ORIGIN}},
                        {"headers": {"Origin": ORIGIN}, "json": {"terminal": "ignored"}}):
            response = await self.client.post("/public/market-feed/start", **options)
            self.assertEqual(response.status_code, 403)
        self.assertEqual((await self.client.post("/mt5/start", headers={"Origin": ORIGIN})).status_code, 403)

    async def test_enabled_policy_or_bound_starter_cannot_construct_visitor_app(self):
        enabled = replace(self.base.policy, market_start_enabled=True)
        with self.assertRaisesRegex(ValueError, "Windows-local"):
            ReadOnlyApplication(enabled, self.base.services, self.base.websocket_app)

        async def inspect_pair():
            return "stopped"

        async def launch_pair():
            self.fail("Visitor API invoked the worker launcher")

        starter = MarketFeedBootstrap("fixture", inspect_pair, launch_pair)
        with self.assertRaisesRegex(ValueError, "Windows-local"):
            ReadOnlyApplication(self.base.policy, self.base.services, self.base.websocket_app, starter)


if __name__ == "__main__":
    unittest.main()
