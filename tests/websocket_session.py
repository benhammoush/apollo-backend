"""Drive synthetic ASGI WebSockets with explicit, awaited shutdown instead of cancellation."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
import json
from typing import Any, cast

from starlette.types import ASGIApp, Message


@dataclass
class WebSocketPeer:
    """An in-process test peer; unexpected close/protocol messages remain test failures."""

    incoming: asyncio.Queue[Message]
    outgoing: asyncio.Queue[Message]

    async def send_text(self, text: str) -> None:
        """Deliver one client message to the actual application receive callable."""
        await self.incoming.put({"type": "websocket.receive", "text": text})

    async def send_json(self, value: dict[str, Any]) -> None:
        """Send a JSON protocol command without altering its selections or fields."""
        await self.send_text(json.dumps(value))

    async def receive_text(self) -> str:
        """Require a text event within the existing two-second synthetic read budget."""
        message = await asyncio.wait_for(self.outgoing.get(), 2)
        if message["type"] != "websocket.send" or not isinstance(message.get("text"), str):
            raise AssertionError("Expected an ASGI WebSocket text event")
        return cast(str, message["text"])

    async def receive_json(self) -> dict[str, Any]:
        """Decode an object response; malformed JSON or a non-object must fail assertions."""
        value = json.loads(await self.receive_text())
        if not isinstance(value, dict):
            raise AssertionError("Expected a JSON object response")
        return cast(dict[str, Any], value)


@asynccontextmanager
async def websocket_session(app: ASGIApp, origin: str) -> AsyncIterator[WebSocketPeer]:
    """Connect a real ASGI task, then deliver disconnect and join it even on assertion failure."""
    peer = WebSocketPeer(asyncio.Queue(), asyncio.Queue())
    scope = {
        "type": "websocket",
        "asgi": {"version": "3.0"},
        "scheme": "ws",
        "path": "/ws",
        "raw_path": b"/ws",
        "query_string": b"",
        "root_path": "",
        "server": ("127.0.0.1", 8811),
        "client": ("127.0.0.1", 1),
        "headers": [(b"origin", origin.encode())],
        "subprotocols": [],
    }
    task = asyncio.ensure_future(app(scope, peer.incoming.get, peer.outgoing.put))
    try:
        await peer.incoming.put({"type": "websocket.connect"})
        if (await asyncio.wait_for(peer.outgoing.get(), 2))["type"] != "websocket.accept":
            raise AssertionError("Expected the selected origin to be accepted")
        yield peer
    finally:
        await peer.incoming.put({"type": "websocket.disconnect", "code": 1000})
        await asyncio.wait_for(task, 3)
