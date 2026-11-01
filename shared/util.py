"""
shared/util.py
==============
Shared utility functions used across the api/, live/, and workers/ packages.
"""

from __future__ import annotations


def sanitize_source(raw: str) -> str:
    """Sanitize a broker/source name to a safe filesystem component.

    Keeps alphanumerics, hyphens, and underscores; replaces spaces with
    underscores; drops all other characters.  Returns '' on empty input.
    """
    out = ""
    for c in raw:
        if c.isalnum() or c in ("-", "_"):
            out += c
        elif c == " ":
            out += "_"
    return out
