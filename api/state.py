"""
api/state.py
============
Global state for feed, worker, and WebSocket management.
Extracted from main.py to break circular imports and reduce coupling.
"""

from typing import Callable, Dict

from .websocket import WebSocketManager

# ── Feed timing state ──────────────────────────────────────────────────
last_push_time: Dict[str, float] = {}
last_bar_time: Dict[str, float] = {}
last_tick_time: Dict[str, float] = {}
last_bar_time_sym: Dict[str, float] = {}
last_forming_time_sym: Dict[str, float] = {}

# ── Feed control state ─────────────────────────────────────────────────
paused_sources: set[str] = set()

# ── History download progress ───────────────────────────────────────────
history_progress: Dict[str, Dict[str, dict]] = {}

# ── Shared manager instances ────────────────────────────────────────────
ws_manager = WebSocketManager()
feed_tracker = None  # Set by main.py lifespan; FeedHealthTracker instance

# Server-owned private integration points. Market ingestion/delivery needs no
# strategy hook; a private profile may explicitly install synchronous callbacks.
# None means disabled, not a failing/unimplemented strategy invocation.
live_tick_callback: Callable[..., None] | None = None
live_bar_callback: Callable[..., None] | None = None
