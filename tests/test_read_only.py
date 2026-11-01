"""Offline visitor-boundary checks; no operational app/lifespan or MT5 access."""

import asyncio
from dataclasses import replace
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import tempfile
import threading
import unittest
from unittest.mock import AsyncMock, patch

import httpx
from starlette.testclient import TestClient
from starlette.websockets import WebSocketDisconnect

from api.read_only import ReadOnlyApplication, ReadOnlyPolicy, SelectedResult
from api.read_only_fixture import fixture_application, fixture_websocket, FixtureServices
from api.read_only_live import LiveReadServices
from api.result_contracts import DatasetDescriptor, SCHEMA_VERSION


ORIGIN = "http://127.0.0.1:5181"


class ReadOnlyHttpTests(unittest.IsolatedAsyncioTestCase):
    """Default-deny HTTP methods, routes, selection, budgets and sanitized errors."""

    async def asyncSetUp(self):
        """Construct only disposable synthetic files and an in-process ASGI client."""
        self.fixture = fixture_application((ORIGIN,))
        self.app = self.fixture.__enter__()
        self.client = httpx.AsyncClient(transport=httpx.ASGITransport(app=self.app), base_url=ORIGIN)

    async def asyncTearDown(self):
        """Release fixture resources without contacting or stopping any listener."""
        await self.client.aclose()
        self.fixture.__exit__(None, None, None)

    async def test_capabilities_pairs_aliases_and_api_prefix(self):
        """Visitor mode is server-owned and contains selected public names only."""
        self.assertEqual((await self.client.get("/api/capabilities")).json(), {"read_only": True, "fixture": True})
        self.assertEqual((await self.client.get("/pairs")).json(), [{"symbol": "EURUSD"}])
        self.assertEqual((await self.client.get("/data/brokers")).json(), {"brokers": ["demo"]})
        self.assertEqual((await self.client.get("/capabilities?profile=operator")).status_code, 422)

    async def test_private_routes_and_methods_are_denied_even_with_spoofed_roles(self):
        """No verb or role/profile header grants visitor operational capabilities."""
        paths = ["/agents/bb/run", "/jobs", "/jobs/fixture/cancel", "/storage/private", "/storage/file",
                 "/storage/export/download", "/mt5", "/mt5/orders", "/internal/feed/notify",
                 "/internal/worker/register", "/logs", "/runs", "/unit-tests", "/chat",
                 "/feed/status", "/docs", "/openapi.json"]
        for path in paths:
            response = await self.client.get(path, headers={"X-Role": "operator", "X-Profile": "internal"})
            self.assertGreaterEqual(response.status_code, 400, path)
        for method in ("POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"):
            response = await self.client.request(method, "/pairs")
            self.assertGreaterEqual(response.status_code, 400, method)

    async def test_ambiguous_paths_origins_and_unknown_query_options_fail_closed(self):
        """Aliases cannot bypass policy; invalid queries never reach market services."""
        for path in ("/api/api/pairs", "/api//pairs", "/api/%2e%2e/pairs", "/pairs/", "/market/EURUSD?worker_port=123"):
            response = await self.client.get(path)
            self.assertGreaterEqual(response.status_code, 400, path)
        self.assertEqual((await self.client.get("/pairs", headers={"Origin": "https://unknown.invalid"})).status_code, 403)

    async def test_exact_origin_cors_covers_success_and_error_responses(self):
        """Browsers can read sanitized errors, not gain new route or method access."""
        for method, path, status in (("GET", "/pairs", 200), ("GET", "/jobs", 404),
                                     ("POST", "/pairs", 403), ("GET", "/api//pairs", 404),
                                     ("GET", "/pairs?unknown=1", 422)):
            response = await self.client.request(method, path, headers={"Origin": ORIGIN})
            self.assertEqual(response.status_code, status)
            self.assertEqual(response.headers["access-control-allow-origin"], ORIGIN)
            self.assertIn("Origin", response.headers["vary"])
            self.assertEqual(response.headers["x-content-type-options"], "nosniff")
            self.assertEqual(response.headers["referrer-policy"], "no-referrer")
            self.assertNotIn("access-control-allow-credentials", response.headers)
        for origin in (None, "https://unknown.invalid", "null", ORIGIN + "/", ORIGIN + ".evil"):
            response = await self.client.get("/pairs", headers={} if origin is None else {"Origin": origin})
            self.assertEqual(response.status_code, 200 if origin is None else 403)
            self.assertNotIn("access-control-allow-origin", response.headers)
            self.assertIn("Origin", response.headers["vary"])

    async def test_cors_preflight_is_read_only_and_does_not_use_work_quota(self):
        """Preflight checks routing only; no services, workers, or quota consumption."""
        with patch.object(self.app.services, "market", new=AsyncMock()) as market:
            for path in ("/pairs", "/api/pairs", "/market/EURUSD", "/api/market/EURUSD"):
                response = await self.client.options(path, headers={
                    "Origin": ORIGIN, "Access-Control-Request-Method": "GET",
                    "Access-Control-Request-Headers": "X-Query-Trigger, X-Query-Interval",
                })
                self.assertEqual(response.status_code, 204)
                self.assertEqual(response.headers["access-control-allow-origin"], ORIGIN)
                self.assertEqual(response.headers["access-control-allow-methods"], "GET")
                self.assertNotIn("*", response.headers["access-control-allow-headers"])
                self.assertNotIn("access-control-allow-credentials", response.headers)
                self.assertIn("Origin", response.headers["vary"])
                self.assertIn("Access-Control-Request-Headers", response.headers["vary"])
            market.assert_not_awaited()
        self.assertEqual(self.app._active, {"metadata": 0, "work": 0})
        self.assertTrue(all(not lane for lane in self.app._arrivals.values()))

    async def test_cors_preflight_denies_private_routes_methods_origins_and_headers(self):
        """A preflight cannot authorize worker startup or broaden the visitor contract."""
        valid = {"Origin": ORIGIN, "Access-Control-Request-Method": "GET"}
        for path in ("/jobs", "/public/market-feed/start", "/api/public/market-feed/start",
                     "/market/UNKNOWN", "/pairs/", "/ws", "/api/api/pairs", "/api//pairs"):
            self.assertGreaterEqual((await self.client.options(path, headers=valid)).status_code, 400, path)
        for changes in ({"Origin": "https://unknown.invalid"}, {"Origin": "null"},
                        {"Access-Control-Request-Method": "POST"},
                        {"Access-Control-Request-Headers": "Authorization"},
                        {"Access-Control-Request-Headers": "X-Query-Trigger, X-Role"},
                        {"Access-Control-Request-Headers": "X-Query-Trigger,"},
                        {"Access-Control-Request-Private-Network": "true"}):
            response = await self.client.options("/pairs", headers=valid | changes)
            self.assertEqual(response.status_code, 403)
            self.assertNotIn("access-control-allow-methods", response.headers)
        for headers in ({}, {"Origin": ORIGIN}, {"Access-Control-Request-Method": "GET"}):
            self.assertEqual((await self.client.options("/pairs", headers=headers)).status_code, 403)
        self.app.seal()
        self.assertEqual((await self.client.options("/pairs", headers=valid)).status_code, 503)

    async def test_ngrok_warning_header_allows_only_existing_visitor_get_contract(self):
        """The warning bypass is not authentication and cannot authorize origins or writes."""
        headers = {"Origin": ORIGIN, "Access-Control-Request-Method": "GET",
                   "Access-Control-Request-Headers": "ngrok-skip-browser-warning, X-Query-Trigger"}
        response = await self.client.options("/health", headers=headers)
        self.assertEqual(response.status_code, 204)
        self.assertEqual(response.headers["access-control-allow-origin"], ORIGIN)
        self.assertEqual(response.headers["access-control-allow-methods"], "GET")
        self.assertIn("ngrok-skip-browser-warning", response.headers["access-control-allow-headers"])
        for changes in ({"Origin": "https://unknown.invalid"},
                        {"Access-Control-Request-Method": "POST"},
                        {"Access-Control-Request-Headers": "ngrok-skip-browser-warning, Authorization"}):
            self.assertEqual((await self.client.options("/health", headers=headers | changes)).status_code, 403)
        actual = {"Origin": ORIGIN, "ngrok-skip-browser-warning": "1"}
        self.assertEqual((await self.client.get("/health", headers=actual)).status_code, 200)
        self.assertEqual((await self.client.get("/health", headers=actual | {"Origin": "https://unknown.invalid"})).status_code, 403)
        self.assertEqual((await self.client.post("/public/market-feed/start", headers=actual)).status_code, 403)
        self.assertEqual((await self.client.options("/public/market-feed/start", headers=headers)).status_code, 403)
        self.assertEqual((await self.client.get("/mt5/terminals_list", headers=actual)).status_code, 404)

    async def test_market_selection_date_limits_and_error_sanitization(self):
        """Invalid sources/symbols/dates are rejected rather than substituted."""
        with patch.object(self.app.services, "market", new=AsyncMock(return_value=[])) as market:
            for query in ("source=unknown", "tf=UNKNOWN", "start=2024-02-01&end=2024-01-01",
                          "tf=M1&start=2024-01-01&end=2024-02-01", "tf=H1&tf=M1", "start=invalid"):
                self.assertGreaterEqual((await self.client.get("/market/EURUSD?" + query)).status_code, 400)
            self.assertEqual((await self.client.get("/market/UNKNOWN")).status_code, 404)
            market.assert_not_awaited()
            response = await self.client.get("/market/EURUSD?source=demo&tf=H1&start=2024-01-01&end=2024-01-02")
            self.assertEqual(response.status_code, 200)
            self.assertEqual(market.call_args.args[2], "synthetic-source")
        with patch.object(self.app.services, "market", new=AsyncMock(side_effect=RuntimeError("fixture-private-path"))):
            response = await self.client.get("/market/EURUSD")
            self.assertEqual(response.status_code, 503)
            self.assertNotIn("fixture-private-path", response.text)

    async def test_selected_result_pages_strip_unknown_metadata_and_keep_missing_values(self):
        """Catalog and table reads never publish arbitrary file or companion metadata."""
        catalog = (await self.client.get("/public/results")).json()
        self.assertEqual(len(catalog), 8)
        self.assertNotIn(str(self.app.policy.result_root), json.dumps(catalog))
        selection = next(r for r in self.app.policy.results if r.description.result_type == "triggers")
        response = await self.client.get(f"/public/results/{selection.description.dataset_id}?limit=1")
        self.assertEqual(response.status_code, 200)
        row = response.json()["rows"][0]
        self.assertIsNone(row["atr_short"])
        self.assertIsNone(row["bkt_short"])
        self.assertEqual(row["p_long"], 0)
        self.assertEqual(row["p_short"], "insufficient_bars")
        selection.path.with_suffix(".meta.json").write_text('{"fixture_private": true}', encoding="utf-8")
        self.assertNotIn("fixture_private", (await self.client.get(f"/public/results/{selection.description.dataset_id}")).text)
        self.assertEqual((await self.client.get("/public/results/unselected")).status_code, 404)
        self.assertEqual((await self.client.get("/public/results/demo-triggers?offset=-1")).status_code, 422)
        self.assertEqual((await self.client.get("/public/results/demo-triggers?limit=999999")).status_code, 422)

    async def test_original_h1_h4_calendar_windows_fit_default_budgets_without_truncation(self):
        """Inclusive date endpoints admit original UI windows, not one extra day."""
        end = datetime(2024, 6, 1, tzinfo=timezone.utc).date()
        # Model initial, centered navigation and older-chunk date serialization.
        for tf, window in (("H1", 730), ("H4", 1825)):
            for days, expected in ((window, 200), (window + 1, 422)):
                with self.subTest(tf=tf, days=days):
                    start = end - timedelta(days=days)
                    with patch.object(self.app.services, "market", new=AsyncMock(return_value=[])) as market:
                        response = await self.client.get(
                            f"/market/EURUSD?tf={tf}&start={start.isoformat()}&end={end.isoformat()}")
                        self.assertEqual(response.status_code, expected)
                        if expected == 200:
                            self.assertEqual(market.call_args.args[3:], (start.isoformat(), end.isoformat()))
                        else:
                            market.assert_not_awaited()
        # Explicit smaller selections stay authoritative; never clamp/fake rows.
        self.app.policy = replace(self.app.policy, history_days={"H1": 3})
        with patch.object(self.app.services, "market", new=AsyncMock(return_value=[])) as market:
            self.assertEqual((await self.client.get(
                "/market/EURUSD?tf=H1&start=2024-01-01&end=2024-01-04")).status_code, 422)
            market.assert_not_awaited()

    async def test_overlays_use_selected_ids_and_enforce_source_symbol_and_time_window(self):
        """Existing chart shapes are adapted without generic folder/file access."""
        catalog = (await self.client.get("/agents/folders/list")).json()
        self.assertTrue(any(row["name"] == "demo-families" for row in catalog))
        path = "/agents/Triggers_BT/folders/demo-triggers/EURUSD/data"
        response = await self.client.get(path + "?broker=demo&start=1704067200&end=1704153600")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(len(response.json()["triggers"]), 1)
        self.assertEqual((await self.client.get(path + "?broker=unknown")).status_code, 422)
        self.assertEqual((await self.client.get(path.replace("EURUSD", "UNKNOWN"))).status_code, 404)
        self.assertEqual((await self.client.get(path + "?start=nan")).status_code, 422)
        rows = (await self.client.get("/agents/bb/EURUSD/series?tf=M15&start=2024-01-01&end=2024-01-02")).json()
        self.assertIs(type(rows[0]["time"]), int)

    async def test_outside_root_oversized_files_and_responses_are_rejected(self):
        """Selected IDs still require resolved containment and byte/row limits."""
        original = self.app.policy.results[0]
        self.app.policy = replace(self.app.policy, results=(SelectedResult(original.description, Path("outside.csv").resolve()),))
        self.assertEqual((await self.client.get("/public/results/demo-bb")).status_code, 404)
        self.app.policy = replace(self.app.policy, results=(original,), max_file_bytes=1)
        self.assertEqual((await self.client.get("/public/results/demo-bb")).status_code, 413)
        self.app.policy = replace(self.app.policy, max_rows=1)
        with patch.object(self.app.services, "market", new=AsyncMock(return_value=[{}, {}])):
            self.assertEqual((await self.client.get("/market/EURUSD")).status_code, 413)

    async def test_multi_symbol_selections_are_not_offered_as_single_symbol_overlays(self):
        """A chart adapter must not mix a selected table's other instruments."""
        selected = tuple(SelectedResult(replace(r.description, symbols=("EURUSD", "GBPUSD")), r.path)
                         for r in self.app.policy.results)
        self.app.policy = replace(self.app.policy, symbols=("EURUSD", "GBPUSD"), results=selected)
        self.assertEqual((await self.client.get("/agents/folders/list")).json(), [])
        self.assertEqual((await self.client.get("/agents/Triggers_BT/folders/demo-triggers/EURUSD/data")).status_code, 404)
        self.assertEqual((await self.client.get("/agents/bb/EURUSD/series?tf=M15")).status_code, 404)

    async def test_overload_is_rejected_without_queuing_additional_reads(self):
        """A saturated visitor request cannot enqueue unbounded history work."""
        entered, release = asyncio.Event(), asyncio.Event()
        async def blocked(*args):
            """Stand in for one slow isolated history request."""
            entered.set()
            await release.wait()
            return []
        self.app.policy = replace(self.app.policy, max_inflight=1)
        with patch.object(self.app.services, "market", new=blocked):
            first = asyncio.create_task(self.client.get("/market/EURUSD"))
            await entered.wait()
            self.assertEqual((await self.client.get("/market/EURUSD")).status_code, 429)
            release.set()
            self.assertEqual((await first).status_code, 200)

    async def test_private_pollers_do_not_consume_public_read_admission(self):
        """Retained original-UI job/status pollers are denied before either read lane."""
        paths = ("/api/agents/status", "/api/jobs?status=running&limit=500", "/api/feed/history/status")
        responses = await asyncio.gather(*(self.client.get(path) for path in paths * 10))
        self.assertTrue(all(response.status_code == 404 for response in responses))
        self.assertEqual(self.app._active, {"metadata": 0, "work": 0})
        self.assertFalse(self.app._arrivals["metadata"])
        self.assertFalse(self.app._arrivals["work"])
        self.assertEqual((await self.client.get("/api/pairs")).status_code, 200)

    async def test_unselected_chart_symbols_are_rejected_before_work_admission(self):
        """Unselected persisted chart slots cannot consume or compete for read quota."""
        paths = ("/api/market/UNKNOWN", "/api/levels/UNKNOWN",
                 "/api/agents/bb/UNKNOWN/series?tf=M15",
                 "/api/agents/Triggers_BT/folders/demo-triggers/UNKNOWN/data")
        with patch.object(self.app.services, "market", new=AsyncMock(return_value=[])) as market, \
             patch.object(self.app, "_result_rows", side_effect=AssertionError("Unselected file read forbidden")):
            responses = await asyncio.gather(*(self.client.get(path) for path in paths * 8))
            self.assertTrue(all(response.status_code == 404 for response in responses))
            self.assertFalse(self.app._arrivals["work"])
            self.assertEqual(self.app._active["work"], 0)
            market.assert_not_awaited()
        self.app._arrivals["work"].extend([asyncio.get_running_loop().time()] * 20)
        self.assertEqual((await self.client.get("/api/market/UNKNOWN")).status_code, 404)

    async def test_unselected_chart_symbols_still_return_404_when_work_capacity_is_full(self):
        """Unsupported symbols are unavailable, not incorrectly reported as overload."""
        entered, release = asyncio.Event(), asyncio.Event()

        async def blocked(*args):
            """Hold the only admitted history slot without touching a real worker."""
            entered.set()
            await release.wait()
            return []

        self.app.policy = replace(self.app.policy, max_inflight=1)
        with patch.object(self.app.services, "market", new=AsyncMock(side_effect=blocked)) as market:
            first = asyncio.create_task(self.client.get("/market/EURUSD"))
            try:
                await asyncio.wait_for(entered.wait(), timeout=5)
                self.assertEqual((await self.client.get("/market/UNKNOWN")).status_code, 404)
                self.assertEqual((await self.client.get("/market/EURUSD")).status_code, 429)
                self.assertEqual(market.await_count, 1)
            finally:
                release.set()
                await first

    async def test_parallel_startup_reads_succeed_while_history_capacity_is_full(self):
        """A slow history request cannot make startup symbol/catalog reads return 429."""
        entered, release = asyncio.Event(), asyncio.Event()

        async def blocked(*args):
            """Keep one mocked expensive read active while the UI starts."""
            entered.set()
            await release.wait()
            return []

        self.app.policy = replace(self.app.policy, max_inflight=1)
        with patch.object(self.app.services, "market", new=blocked):
            first = asyncio.create_task(self.client.get("/market/EURUSD"))
            try:
                await asyncio.wait_for(entered.wait(), timeout=5)
                paths = ("/pairs", "/health", "/capabilities", "/data/brokers", "/storage/tree",
                         "/public/results", "/agents/folders/list", "/agents/status", "/jobs?status=running&limit=500")
                responses = await asyncio.gather(*(self.client.get(path) for path in paths))
                self.assertEqual([response.status_code for response in responses], [200] * 7 + [404, 404])
                self.assertEqual((await self.client.get("/market/EURUSD")).status_code, 429)
            finally:
                release.set()
                await first
            self.assertEqual(first.result().status_code, 200)
        self.assertEqual(self.app._active, {"metadata": 0, "work": 0})

    async def test_metadata_capacity_is_bounded_and_independent_of_expensive_work(self):
        """The separate cheap lane is bounded, rather than exempting it from admission."""
        entered, release = asyncio.Event(), asyncio.Event()

        async def blocked_sources():
            """Stand in for a slow source-availability lookup without live process reads."""
            entered.set()
            await release.wait()
            return {"synthetic-source"}

        self.app.policy = replace(self.app.policy, max_metadata_inflight=1)
        with patch.object(self.app.services, "available_sources", new=blocked_sources), \
             patch.object(self.app.services, "market", new=AsyncMock(return_value=[])):
            first = asyncio.create_task(self.client.get("/data/brokers"))
            try:
                await asyncio.wait_for(entered.wait(), timeout=5)
                self.assertEqual((await self.client.get("/pairs")).status_code, 429)
                self.assertEqual((await self.client.get("/market/EURUSD")).status_code, 200)
                self.assertEqual((await self.client.get("/jobs")).status_code, 404)
            finally:
                release.set()
                await first
        self.assertEqual(first.result().status_code, 200)
        self.assertEqual(self.app._active, {"metadata": 0, "work": 0})

    async def test_history_rate_budget_does_not_exhaust_metadata_budget(self):
        """Even a full work-lane rate window leaves symbol reads in their own window."""
        now = asyncio.get_running_loop().time()
        self.app._arrivals["work"].extend([now] * 20)
        with patch.object(self.app.services, "market", new=AsyncMock(return_value=[])) as market:
            self.assertEqual((await self.client.get("/market/EURUSD")).status_code, 429)
            self.assertEqual((await self.client.get("/pairs")).status_code, 200)
            market.assert_not_awaited()

    async def test_unclassified_get_route_remains_denied_before_its_handler(self):
        """Adding a route cannot silently turn it into a cheap public capability."""
        reached = False

        async def unclassified():
            """Represent a mistakenly added route that must not become public."""
            nonlocal reached
            reached = True
            return {"fixture": True}

        self.app.http.add_api_route("/unclassified-fixture", unclassified, methods=["GET"])
        self.assertEqual((await self.client.get("/unclassified-fixture")).status_code, 404)
        self.assertFalse(reached)

    async def test_metadata_limit_must_be_an_explicit_positive_integer(self):
        """Unsafe metadata-admission configuration cannot silently disable limits."""
        for value in (0, -1, True):
            with self.subTest(value=value), self.assertRaises(ValueError):
                replace(self.app.policy, max_metadata_inflight=value)

    async def test_executor_work_keeps_admission_after_timeout_or_caller_cancellation(self):
        """Expired HTTP waiters cannot submit extra file work while its thread lives."""
        self.app.policy = replace(self.app.policy, max_inflight=1, request_timeout=1)
        original = self.app._result_rows
        loop = asyncio.get_running_loop()
        for mode in ("timeout", "cancel"):
            with self.subTest(mode=mode):
                entered, release = asyncio.Event(), threading.Event()

                def blocked(*args):
                    """Hold actual executor work past the HTTP waiter's lifetime."""
                    loop.call_soon_threadsafe(entered.set)
                    if not release.wait(timeout=5):
                        raise AssertionError("Test reader was not released")
                    return original(*args)

                with patch.object(self.app, "_result_rows", new=blocked):
                    first = asyncio.create_task(self.client.get("/public/results/demo-bb"))
                    try:
                        await asyncio.wait_for(entered.wait(), timeout=3)
                        if mode == "cancel":
                            first.cancel()
                            with self.assertRaises(asyncio.CancelledError):
                                await first
                        else:
                            self.assertEqual((await first).status_code, 503)
                        self.assertEqual(self.app._active["work"], 1)
                        self.assertEqual(len(self.app._http_tasks), 1)
                        self.assertEqual((await self.client.get("/public/results/demo-bb")).status_code, 429)
                        self.assertEqual((await self.client.get("/pairs")).status_code, 200)
                    finally:
                        release.set()
                        await asyncio.gather(first, return_exceptions=True)
                        if self.app._http_tasks:
                            await asyncio.wait_for(asyncio.gather(*tuple(self.app._http_tasks)), timeout=3)
                    self.assertEqual(self.app._active["work"], 0)
                    self.assertFalse(self.app._http_tasks)
                    self.assertEqual((await self.client.get("/public/results/demo-bb")).status_code, 200)

    async def test_real_adapter_decoder_remains_charged_beyond_http_timeout(self):
        """The live adapter must not detach its decoder through an inner timeout."""
        from importlib import import_module
        market = import_module('api.market_read')

        self.app.policy = replace(self.app.policy, max_inflight=1, request_timeout=1)
        self.app.services = LiveReadServices(self.app.policy)
        entered, release = asyncio.Event(), threading.Event()
        loop = asyncio.get_running_loop()
        original = market._decode_ohlcv_response

        def decode(raw):
            """Delay a real decoder using only mocked worker bytes, never an MT5 call."""
            loop.call_soon_threadsafe(entered.set)
            if not release.wait(timeout=5):
                raise AssertionError("Test decoder was not released")
            return original(raw)

        transport = httpx.MockTransport(lambda request: httpx.Response(200, json={
            "symbol": "EURUSD", "tf": "H1", "bars": [{"time": 1704067200,
            "open": 1.25, "high": 1.26, "low": 1.24, "close": 1.255, "volume": 10}],
        }))
        async with httpx.AsyncClient(transport=transport) as worker:
            with patch.object(self.app.services, "_port", new=AsyncMock(return_value=1234)), \
                 patch.object(market, "_get_client", return_value=worker), \
                 patch.object(market, "_decode_ohlcv_response", side_effect=decode) as decoder:
                first = asyncio.create_task(self.client.get("/market/EURUSD"))
                try:
                    await asyncio.wait_for(entered.wait(), timeout=3)
                    self.assertEqual((await first).status_code, 503)
                    self.assertEqual(self.app._active["work"], 1)
                    self.assertEqual(len(self.app.services._pending), 1)
                    self.assertEqual((await self.client.get("/market/EURUSD")).status_code, 429)
                finally:
                    release.set()
                    await asyncio.gather(first, return_exceptions=True)
                    if self.app._http_tasks:
                        await asyncio.wait_for(asyncio.gather(*tuple(self.app._http_tasks)), timeout=3)
                self.assertFalse(self.app.services._pending)
                self.assertEqual(self.app._active["work"], 0)
                self.assertEqual((await self.client.get("/market/EURUSD")).status_code, 200)
                self.assertEqual(decoder.call_count, 1)


class ReadOnlyWebSocketTests(unittest.TestCase):
    """Exercise origins, subscriptions, private-event suppression and reconnection."""

    def test_only_filtered_market_events_and_ping_are_delivered(self):
        """Filter the fixture and explicitly join producer shutdown, avoiding a client cancel race."""
        async def check():
            with fixture_application((ORIGIN,)) as app:
                for path in ('/ws', '/api/ws'):
                    incoming, outgoing = asyncio.Queue(), asyncio.Queue()
                    scope = {'type': 'websocket', 'asgi': {'version': '3.0'}, 'scheme': 'ws',
                        'path': path, 'raw_path': path.encode(), 'query_string': b'', 'root_path': '',
                        'server': ('127.0.0.1', 8811), 'client': ('127.0.0.1', 1),
                        'headers': [(b'origin', ORIGIN.encode())], 'subprotocols': []}
                    task = asyncio.create_task(app(scope, incoming.get, outgoing.put))
                    try:
                        await incoming.put({'type': 'websocket.connect'})
                        self.assertEqual((await asyncio.wait_for(outgoing.get(), 2))['type'], 'websocket.accept')
                        await incoming.put({'type': 'websocket.receive', 'text': json.dumps({'type': 'subscribe', 'symbols': ['EURUSD']})})
                        tick = json.loads((await asyncio.wait_for(outgoing.get(), 2))['text'])
                        self.assertEqual(tick['type'], 'tick')
                        self.assertEqual(tick['data']['source'], 'demo')
                        self.assertNotIn('synthetic-source', json.dumps(tick))
                        await incoming.put({'type': 'websocket.receive', 'text': 'ping'})
                        self.assertEqual((await asyncio.wait_for(outgoing.get(), 2))['text'], 'pong')
                    finally:
                        await incoming.put({'type': 'websocket.disconnect', 'code': 1000})
                        await asyncio.wait_for(task, 3)
        asyncio.run(check())

    def test_unknown_origins_commands_and_malformed_or_oversized_messages_are_rejected(self):
        """Direct requests and malformed protocol variants cannot acquire controls."""
        with fixture_application((ORIGIN,)) as app, TestClient(app) as client:
            with self.assertRaises(WebSocketDisconnect):
                with client.websocket_connect("/ws", headers={"Origin": "https://unknown.invalid"}):
                    pass
            for payload in ('{"type":"cancel","symbols":[]}', '{"type":"subscribe","symbols":[1]}',
                            '{"type":"subscribe","symbols":{}}',
                            '{"type":"subscribe","symbols":["EURUSD"],"profile":"operator"}', "x" * 2049):
                with client.websocket_connect("/ws", headers={"Origin": ORIGIN}) as ws:
                    ws.send_text(payload)
                    with self.assertRaises(WebSocketDisconnect):
                        ws.receive_text()

    def test_unselected_symbols_are_denied_without_closing_or_forwarding_them(self):
        """Mixed persisted chart slots retain admitted live data without reconnect loops."""
        forwarded = []

        async def traced(scope, receive, send):
            """Observe only messages that actually reach the synthetic producer."""
            async def capture():
                """Capture canonical channel requests, excluding transport keepalive."""
                message = await receive()
                if message["type"] == "websocket.receive" and message.get("text") != "ping":
                    forwarded.append(json.loads(message["text"]))
                return message
            await fixture_websocket(scope, capture, send)

        with fixture_application((ORIGIN,)) as app:
            app.websocket_app = traced
            with TestClient(app) as client, client.websocket_connect("/ws", headers={"Origin": ORIGIN}) as ws:
                ws.send_json({"type": "subscribe", "symbols": ["EURUSD", "PRIVATE_FIXTURE_ONLY", "GBPUSD"]})
                self.assertEqual(ws.receive_json(), {
                    "type": "subscription_status", "status": "partial", "symbols": ["EURUSD"],
                })
                self.assertEqual(ws.receive_json()["data"]["symbol"], "EURUSD")
                self.assertEqual(forwarded[-1], {"type": "subscribe", "symbols": ["EURUSD"]})
                self.assertEqual(app._connections, 1)
                # An all-unavailable replacement clears old subscriptions rather
                # than silently reusing the previous source/symbol selection.
                ws.send_json({"type": "subscribe", "symbols": ["PRIVATE_FIXTURE_ONLY"]})
                self.assertEqual(ws.receive_json(), {
                    "type": "subscription_status", "status": "unavailable", "symbols": [],
                })
                ws.send_text("ping")
                self.assertEqual(ws.receive_text(), "pong")
                self.assertEqual(forwarded[-1], {"type": "subscribe", "symbols": []})
                ws.send_json({"type": "subscribe", "symbols": ["EURUSD"]})
                self.assertEqual(ws.receive_json()["type"], "tick")
                ws.send_json({"type": "unsubscribe", "symbols": ["EURUSD", "UNKNOWN"]})
                self.assertEqual(ws.receive_json()["status"], "partial")
                ws.send_text("ping")
                self.assertEqual(ws.receive_text(), "pong")
                self.assertEqual(forwarded[-1], {"type": "unsubscribe", "symbols": ["EURUSD"]})
                self.assertEqual(app._connections, 1)
            self.assertEqual(app._connections, 0)

    def test_subscription_cardinality_rejection_keeps_the_connection_usable(self):
        """Well-formed excessive selections do not bypass limits or force reconnects."""
        forwarded = []

        async def producer(scope, receive, send):
            """Emit on ping to prove the pre-rejection subscription still receives data."""
            await receive()
            await send({"type": "websocket.accept"})
            while True:
                message = await receive()
                if message["type"] == "websocket.disconnect":
                    return
                text = message.get("text")
                if text != "ping":
                    forwarded.append(json.loads(text))
                event = {"type": "tick", "data": {"symbol": "EURUSD", "source": "synthetic-source",
                         "bid": 1.1, "ask": 1.1001, "time_ms": 1704067200000}}
                await send({"type": "websocket.send", "text": json.dumps(event)})
                if text == "ping":
                    await send({"type": "websocket.send", "text": "pong"})

        with fixture_application((ORIGIN,)) as app:
            app.policy = replace(app.policy, max_subscriptions=1)
            app.websocket_app = producer
            with TestClient(app) as client, client.websocket_connect("/ws", headers={"Origin": ORIGIN}) as ws:
                ws.send_json({"type": "subscribe", "symbols": ["EURUSD"]})
                self.assertEqual(ws.receive_json()["type"], "tick")
                ws.send_json({"type": "subscribe", "symbols": ["EURUSD", "UNKNOWN"]})
                self.assertEqual(ws.receive_json(), {
                    "type": "subscription_status", "status": "limit_exceeded", "symbols": [],
                })
                ws.send_text("ping")
                self.assertEqual(ws.receive_json()["type"], "tick")
                self.assertEqual(ws.receive_text(), "pong")
                self.assertEqual(forwarded, [{"type": "subscribe", "symbols": ["EURUSD"]}])
                ws.send_json({"type": "subscribe", "symbols": ["EURUSD"]})
                self.assertEqual(ws.receive_json()["type"], "tick")
                self.assertEqual(app._connections, 1)

    def test_semantic_rejections_still_obey_the_inbound_message_rate_limit(self):
        """Keeping an unavailable subscription alive is not an unlimited reply channel."""
        with fixture_application((ORIGIN,)) as app, TestClient(app) as client:
            with client.websocket_connect("/ws", headers={"Origin": ORIGIN}) as ws:
                for _ in range(60):
                    ws.send_json({"type": "subscribe", "symbols": ["UNKNOWN"]})
                    self.assertEqual(ws.receive_json()["status"], "unavailable")
                ws.send_json({"type": "subscribe", "symbols": ["UNKNOWN"]})
                with self.assertRaises(WebSocketDisconnect):
                    ws.receive_text()

    def test_unscoped_private_and_wrong_source_events_are_suppressed(self):
        """Outbound privacy is type/source/symbol based, not just a symbol argument."""
        with fixture_application((ORIGIN,)) as app:
            self.assertIsNone(app._event({"type": "job_done", "fixture": True}, {"EURUSD"}))
            self.assertIsNone(app._event({"type": "subscription_status", "private_fixture": True}, {"EURUSD"}))
            self.assertIsNone(app._event({"type": "tick", "data": {"symbol": "EURUSD", "source": "unselected"}}, {"EURUSD"}))
            self.assertIsNone(app._event({"type": "tick", "data": {"symbol": "EURUSD", "source": "synthetic-source"}}, set()))
            event = app._event({"type": "forming_bar", "symbol": "EURUSD", "source": "synthetic-source",
                                "bar_open_time": 100, "open": None, "close": 0, "private": {"fixture": True}}, {"EURUSD"})
            self.assertIsNone(event["open"])
            self.assertEqual(event["close"], 0)
            self.assertNotIn("private", event)


class ReadOnlyWebSocketLifecycleTests(unittest.IsolatedAsyncioTestCase):
    """Bound slow/closed transports without leaving producer or connection slots alive."""

    async def test_blocked_data_and_close_sends_release_connection_admission(self):
        """Both send and close deadlines terminate an idle producer without client help."""
        with fixture_application((ORIGIN,)) as app:
            incoming = asyncio.Queue()
            incoming.put_nowait({"type": "websocket.connect"})
            incoming.put_nowait({"type": "websocket.receive", "text": json.dumps({"type": "subscribe", "symbols": ["EURUSD"]})})
            exited = asyncio.Event()
            close_attempts = []
            never = asyncio.Event()

            async def producer(scope, receive, send):
                """Wait for the proxy's synthetic disconnect after producing one tick."""
                try:
                    await receive()
                    await send({"type": "websocket.accept"})
                    await receive()
                    await send({"type": "websocket.send", "text": json.dumps({"type": "tick", "data": {
                        "symbol": "EURUSD", "source": "synthetic-source", "bid": 1.1}})})
                    self.assertEqual((await receive())["type"], "websocket.disconnect")
                finally:
                    exited.set()

            async def blocked_send(message):
                """Accept the handshake but never finish data or close delivery."""
                if message["type"] == "websocket.close":
                    close_attempts.append(message["code"])
                if message["type"] in {"websocket.send", "websocket.close"}:
                    await never.wait()

            app.websocket_app = producer
            scope = {"type": "websocket", "headers": [(b"origin", ORIGIN.encode())], "query_string": b""}
            await asyncio.wait_for(app._websocket(scope, incoming.get, blocked_send), timeout=7)
            self.assertTrue(exited.is_set())
            self.assertEqual(close_attempts, [1013])
            self.assertEqual(app._connections, 0)

    async def test_queue_saturation_closes_without_blocking_the_producer(self):
        """A burst cannot turn the public outbound queue into unbounded work."""
        with fixture_application((ORIGIN,)) as app:
            app.policy = replace(app.policy, ws_queue_size=1)
            incoming = asyncio.Queue()
            incoming.put_nowait({"type": "websocket.connect"})
            incoming.put_nowait({"type": "websocket.receive", "text": json.dumps({"type": "subscribe", "symbols": ["EURUSD"]})})
            produced = asyncio.Event()
            sent = []

            async def producer(scope, receive, send):
                """Generate a synchronous burst, then require a clean disconnect."""
                await receive()
                await send({"type": "websocket.accept"})
                await receive()
                for _ in range(3):
                    await send({"type": "websocket.send", "text": json.dumps({"type": "tick", "data": {
                        "symbol": "EURUSD", "source": "synthetic-source", "bid": 1.1}})})
                produced.set()
                self.assertEqual((await receive())["type"], "websocket.disconnect")

            async def send(message):
                """Record only actual transport output, not discarded queued events."""
                sent.append(message)

            app.websocket_app = producer
            scope = {"type": "websocket", "headers": [(b"origin", ORIGIN.encode())], "query_string": b""}
            await asyncio.wait_for(app._websocket(scope, incoming.get, send), timeout=3)
            self.assertTrue(produced.is_set())
            self.assertIn({"type": "websocket.close", "code": 1013}, sent)
            self.assertEqual(app._connections, 0)


class ReadOnlyLiveAdapterTests(unittest.IsolatedAsyncioTestCase):
    """Check registry source isolation and bounded cache behavior with mocks only."""

    async def test_missing_exact_source_never_falls_back_to_another_worker(self):
        """An existing unrelated history worker does not satisfy public selection."""
        with fixture_application((ORIGIN,)) as app:
            service = LiveReadServices(app.policy)
            workers = {"other": {"worker_type": "mt5_history", "mql5_source": "unselected", "port": 1234}}
            with patch("api.worker_registry.all_workers", new=AsyncMock(return_value=workers)), \
                 patch("api.worker_registry.is_alive", new=AsyncMock(return_value=True)):
                with self.assertRaises(Exception) as caught:
                    await service._port("synthetic-source")
                self.assertEqual(caught.exception.status_code, 503)
                self.assertEqual(await service.available_sources(), set())

    async def test_deduplication_survives_one_waiter_cancellation_and_cache_is_bounded(self):
        """One disconnected visitor cannot cancel a shared read for another visitor."""
        with fixture_application((ORIGIN,)) as app:
            service = LiveReadServices(app.policy)
            entered, release = asyncio.Event(), asyncio.Event()
            async def fetch(*args):
                """A mocked history request with explicit release, never a live worker."""
                entered.set()
                await release.wait()
                return [{"time": 100, "close": 1.25}]
            with patch.object(service, "_port", new=AsyncMock(return_value=1234)), \
                 patch.object(service, "_fetch", new=AsyncMock(side_effect=fetch)) as mocked:
                args = ("EURUSD", "H1", "synthetic-source", "2024-01-01", "2024-01-02")
                first = asyncio.create_task(service.market(*args))
                await entered.wait()
                second = asyncio.create_task(service.market(*args))
                await asyncio.sleep(0)
                first.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await first
                release.set()
                self.assertEqual((await second)[0]["close"], 1.25)
                self.assertEqual(mocked.await_count, 1)
                for day in range(3, 9):
                    await service.market("EURUSD", "H1", "synthetic-source", "2024-01-01", f"2024-01-{day:02}")
                self.assertLessEqual(len(service._cache), 4)
                self.assertFalse(service._pending)


if __name__ == "__main__":
    unittest.main()
