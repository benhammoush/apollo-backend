"""Disabled fixed-target binding contract, not a legacy MT5 spawning adapter.

A real ownership controller must fence private starts/recovery and validate the
same configuration in the child before connection. No such controller is wired
here, and no process, terminal or registry is inspected by importing this module.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
import re
from typing import TYPE_CHECKING

from api.market_bootstrap import MarketFeedBootstrap

if TYPE_CHECKING:
    from api.read_only import ReadOnlyPolicy

MAX_CONFIG_BYTES = 1_000_000


@dataclass(frozen=True)
class MT5TargetBinding:
    """Private server-owned selection; never request parameters or public metadata."""

    terminal_id: str
    source_alias: str
    config_revision: str
    expected_source: str | None = None
    enabled: bool = False

    def __post_init__(self):
        """Validate identities without resolving terminals or reading account data."""
        if type(self.terminal_id) is not str or re.fullmatch(r"term_[0-9a-f]{8}", self.terminal_id) is None:
            raise ValueError("Invalid approved terminal identifier")
        if type(self.source_alias) is not str or re.fullmatch(r"[a-z0-9][a-z0-9_-]{0,63}", self.source_alias) is None:
            raise ValueError("Invalid approved public alias")
        if type(self.config_revision) is not str or re.fullmatch(r"[0-9a-f]{64}", self.config_revision) is None:
            raise ValueError("Invalid target configuration revision")
        if type(self.enabled) is not bool or (self.expected_source is not None
                and (type(self.expected_source) is not str or not 1 <= len(self.expected_source) <= 160
                     or not self.expected_source.strip() or any(ord(char) < 32 or ord(char) == 127 for char in self.expected_source))):
            raise ValueError("Invalid target binding")


def target_revision(config_path: Path, terminal_id: str, *, require_active: bool = False) -> tuple[str, bool]:
    """Hash only known execution metadata, never retain paths in the binding artifact.

    A missing/corrupt/ambiguous config is unavailable, not an empty worker set.
    Unexpected target fields require review rather than hashing possible secrets.
    """
    path = Path(os.path.abspath(config_path))
    if any(node.is_symlink() or node.is_junction() for node in (path, *path.parents)):
        raise ValueError("Linked MT5 target configuration")
    try:
        before = path.stat()
        with path.open("rb") as stream:
            opened = os.fstat(stream.fileno())
            raw = stream.read(MAX_CONFIG_BYTES + 1)
            after_open = os.fstat(stream.fileno())
        after = path.stat()
        identities = [(item.st_dev, item.st_ino, item.st_size, item.st_mtime_ns)
                      for item in (before, opened, after_open, after)]
        if len(raw) > MAX_CONFIG_BYTES or len(set(identities)) != 1:
            raise ValueError("Changed or oversized target configuration")
        config = json.loads(raw)
        terminals = config.get("terminals")
        if type(terminals) is not list or len(terminals) > 100 or any(type(item) is not dict for item in terminals):
            raise ValueError("Invalid target configuration")
        matches = [item for item in terminals if item.get("id") == terminal_id]
        if len(matches) != 1:
            raise ValueError("Approved terminal missing or ambiguous")
        selected = matches[0]
        if set(selected) - {"id", "terminal_path", "active", "label", "created_at"}:
            raise ValueError("Target schema requires additional review")
        terminal_path, active = selected.get("terminal_path"), selected.get("active")
        if type(terminal_path) is not str or not 1 <= len(terminal_path) <= 4096 or type(active) is not bool:
            raise ValueError("Invalid fixed target metadata")
        if require_active and not active:
            raise ValueError("Private operator has disabled this target")
        approved = {"id": terminal_id, "terminal_path": terminal_path, "active": active}
        revision = hashlib.sha256(json.dumps(approved, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()
        return revision, active
    except (OSError, ValueError, TypeError, AttributeError):
        raise ValueError("MT5 target configuration unavailable") from None


class MT5PairOwnership(ABC):
    """Required host interface, not a claim that legacy startup implements ownership.

    Implementations must share a lease with private start/stop/config/recovery,
    detect unregistered children and PID reuse, pin child configuration before
    MT5 initialization, and hold ownership through partial launch/cancellation.
    Legacy/unfenced spawning callbacks must not be used as implementations.
    """

    @abstractmethod
    async def inspect_pair(self, target: MT5TargetBinding) -> str:
        """Return confirmed pair state under the host's real ownership contract."""
        raise NotImplementedError

    @abstractmethod
    async def start_if_absent(self, target: MT5TargetBinding) -> None:
        """Atomically revalidate/claim and start only the fixed approved pair."""
        raise NotImplementedError


def bind_market_bootstrap(target: MT5TargetBinding, policy: ReadOnlyPolicy, config_path: Path,
                          ownership: MT5PairOwnership | None, *, activation_approved: bool = False,
                          approved_config_revision: str | None = None) -> MarketFeedBootstrap:
    """Require explicit promotion, source approval and a reviewed ownership controller.

    No default controller, source inference, generic-management fallback or
    process launch is available. The inspected source must be approved separately;
    a display label is not an MT5 registry/source identity.
    """
    if (activation_approved is not True or target.enabled is not True
            or approved_config_revision != target.config_revision
            or target.expected_source is None or not policy.market_start_enabled
            or policy.sources.get(target.source_alias) != target.expected_source
            or target.source_alias in {item.description.source_id for item in policy.results}
            or not isinstance(ownership, MT5PairOwnership)):
        raise ValueError("Approved activation, exact source and ownership implementation required")
    config_path = Path(os.path.abspath(config_path))

    def unchanged():
        """Reject changed target paths/active state before inspection or launch."""
        current, _ = target_revision(config_path, target.terminal_id, require_active=True)
        if current != target.config_revision:
            raise ValueError("Approved MT5 target configuration changed")

    unchanged()

    async def inspect_pair():
        """Read only the pinned target through the supplied host controller."""
        unchanged()
        return await ownership.inspect_pair(target)

    async def start_pair():
        """The host must repeat this validation atomically inside its shared lease."""
        unchanged()
        await ownership.start_if_absent(target)

    return MarketFeedBootstrap(target.source_alias, inspect_pair, start_pair)
