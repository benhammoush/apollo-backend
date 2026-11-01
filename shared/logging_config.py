"""
Shared logging helpers for Apollo processes.
"""

from __future__ import annotations

import logging
from datetime import datetime


def suppress_httpx_info_logs() -> None:
    """Keep HTTP client logs at WARNING+ to avoid request-line spam."""
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)


def fmt_ts(dt: datetime | str | None) -> str:
    """Format a timestamp as ``YYYY-MM-DD-HH:MM`` for log prefixes.

    Accepts a ``datetime`` object, an ISO-format string, or ``None``.
    """
    if dt is None:
        return "N/A"
    if isinstance(dt, str):
        dt = datetime.fromisoformat(dt)
    return dt.strftime("%Y-%m-%d-%H:%M")
