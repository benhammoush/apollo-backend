"""
api/websocket.py
================
WebSocket connection manager.
Broadcast messages to all connected clients.
"""
from __future__ import annotations

import asyncio
import json
import logging
from typing import Any, Dict, List, Set

from fastapi import WebSocket

logger = logging.getLogger(__name__)


class WebSocketManager:
    MAX_CONNECTIONS = 50
    SEND_TIMEOUT = 5.0  # seconds per client

    def __init__(self):
        self._clients: List[WebSocket] = []
        # Optional per-client symbol filter. None = receive all symbols.
        self._subscriptions: Dict[WebSocket, Set[str]] = {}

    async def connect(self, ws: WebSocket) -> None:
        if len(self._clients) >= self.MAX_CONNECTIONS:
            await ws.close(code=1013, reason="Max connections")
            logger.warning("WS connection rejected: max %d clients", self.MAX_CONNECTIONS)
            return
        await ws.accept()
        self._clients.append(ws)
        self._subscriptions[ws] = set()
        logger.info(f"WS client connected ({len(self._clients)} total)")

    def disconnect(self, ws: WebSocket) -> None:
        if ws in self._clients:
            self._clients.remove(ws)
        self._subscriptions.pop(ws, None)
        logger.info(f"WS client disconnected ({len(self._clients)} remaining)")

    def subscribe(self, ws: WebSocket, symbols: List[str]) -> None:
        """Register the symbols this client wants to receive tick/bar events for."""
        upper = {s.upper() for s in symbols}
        self._subscriptions[ws] = upper

    def unsubscribe(self, ws: WebSocket, symbols: List[str]) -> None:
        """Remove symbols from this client's subscription set."""
        if ws in self._subscriptions:
            for s in symbols:
                self._subscriptions[ws].discard(s.upper())

    async def broadcast(self, message: Dict[str, Any], symbol: str | None = None) -> None:
        """Broadcast message to all clients in parallel.

        If *symbol* is provided, only clients that are subscribed to that symbol
        (or have no subscription filter) receive the message.

        Sends are dispatched via ``asyncio.gather()`` so a slow client does not
        block other clients.  Each send is capped at ``SEND_TIMEOUT`` seconds.
        """
        if not self._clients:
            return

        text = json.dumps(message)
        sym_upper = symbol.upper() if symbol else None

        targets: List[WebSocket] = []
        for ws in self._clients:
            if sym_upper is not None:
                subs = self._subscriptions.get(ws)
                if subs is not None and sym_upper not in subs:
                    continue
            targets.append(ws)

        if not targets:
            return

        async def _safe_send(ws: WebSocket) -> bool:
            try:
                await asyncio.wait_for(ws.send_text(text), timeout=self.SEND_TIMEOUT)
                return True
            except Exception:
                logger.debug("WS send failed for client")
                return False

        results = await asyncio.gather(*[_safe_send(ws) for ws in targets])

        # Remove clients whose send failed
        for ws, ok in zip(targets, results):
            if not ok:
                self.disconnect(ws)
