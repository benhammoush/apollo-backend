"""Child-side validation before MT5 connection or any PID/status publication."""

import hashlib
import json
from pathlib import Path

from api.mt5_binding import MAX_CONFIG_BYTES, target_revision


def pinned_terminal(config_path: Path, terminal_id: str, revision: str) -> dict:
    """Return an immutable execution snapshot; caller holds the operation fence."""
    current, _ = target_revision(config_path, terminal_id, require_active=True)
    if current != revision:
        raise ValueError("MT5 startup pin changed")
    with Path(config_path).open("rb") as stream:
        raw = stream.read(MAX_CONFIG_BYTES + 1)
    if len(raw) > MAX_CONFIG_BYTES:
        raise ValueError("Oversized startup configuration")
    terminals = json.loads(raw)["terminals"]
    matches = [term for term in terminals if term.get("id") == terminal_id]
    if len(matches) != 1:
        raise ValueError("Ambiguous startup target")
    selected = matches[0]
    execution = {key: selected[key] for key in ("id", "terminal_path", "active")}
    actual = hashlib.sha256(json.dumps(execution, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()
    if actual != revision or selected["active"] is not True:
        raise ValueError("MT5 startup pin changed")
    return dict(selected)


def validate_source(actual: str, expected: str | None):
    """Fail before PID/registry/feed publication if the approved source mismatches."""
    if expected is not None and actual != expected:
        raise ValueError("MT5 source does not match approved startup binding")
