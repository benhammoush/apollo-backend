"""Verify deterministic test-peer teardown without suppressing server or assertion errors."""

import unittest

from api.read_only_fixture import fixture_application
from tests.websocket_session import websocket_session


class WebSocketSessionTests(unittest.IsolatedAsyncioTestCase):
    """The ASGI application and writer must finish before a test context is released."""

    async def test_disconnect_joins_the_application_and_releases_admission(self) -> None:
        """A normal close releases all connection admission without TestClient cancellation."""
        origin = "http://127.0.0.1:5181"
        with fixture_application((origin,)) as app:
            async with websocket_session(app, origin) as peer:
                await peer.send_json({"type": "subscribe", "symbols": ["EURUSD"]})
                self.assertEqual((await peer.receive_json())["type"], "tick")
                self.assertEqual(app._connections, 1)
            self.assertEqual(app._connections, 0)

    async def test_assertion_failure_is_propagated_after_application_cleanup(self) -> None:
        """The helper cannot turn a failing assertion into a successful or leaked session."""
        origin = "http://127.0.0.1:5181"
        with fixture_application((origin,)) as app:
            with self.assertRaisesRegex(AssertionError, "synthetic assertion"):
                async with websocket_session(app, origin):
                    raise AssertionError("synthetic assertion")
            self.assertEqual(app._connections, 0)
