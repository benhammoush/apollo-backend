"""
api/worker_registry.py
======================
In-process worker state registry — replaces pids.json TOCTOU coordination.

Workers POST to /internal/worker/register at startup with their PID and port.
The API reads from this registry instead of polling a file.

Registry entry schema:
  {
    "pid":         int,
    "port":        int | None,
    "mql5_source": str,
    "status":      "running",
    "worker_type": "mt5_live" | "mt5_history" | "mt5_ephemeral" | None,
    "job_id":      str | None,    # ephemeral only
    "agent_name":  str | None,    # ephemeral only
    "started_at":  float,         # unix timestamp
  }

All functions are async and protected by a single asyncio.Lock.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import threading
import time
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


class WorkerRegistry:
    """In-process worker state registry — single source of truth for terminal state."""

    def __init__(self) -> None:
        # { worker_id: {pid, port, mql5_source, status, worker_type, ...} }
        self._registry: dict[str, dict] = {}
        self._lock = asyncio.Lock()

        # Terminal status cache — keyed by terminal_id, stores the latest status
        # dict from workers (phase, broker, connected, mql5_source, etc.)
        self._terminal_status: dict[str, dict] = {}

        # Config cache — avoids repeated file I/O in the hot path
        self._terminal_config: dict | None = None
        self._config_mtime: float = 0.0
        self._config_path = Path(__file__).resolve().parent.parent / "cache" / "mt5" / "config.json"

        # Ephemeral event tracking — keyed by "{terminal_id}:{job_id}"
        self._ephemeral_events: dict[str, threading.Event] = {}

    # ------------------------------------------------------------------
    # Worker registration
    # ------------------------------------------------------------------

    async def register(
        self,
        worker_id: str,
        pid: int,
        port: Optional[int] = None,
        mql5_source: Optional[str] = None,
        worker_type: Optional[str] = None,
        job_id: Optional[str] = None,
        agent_name: Optional[str] = None,
        started_at: Optional[float] = None,
        terminal_id: Optional[str] = None,
    ) -> None:
        """Register (or update) a worker entry.

        Args:
            worker_id: Unique worker identifier (e.g. "mt5_live:term_abc123").
            pid: The process ID of the worker.
            port: The HTTP port the worker is listening on.
            mql5_source: The broker/source name supplied by the MQL5 bridge.
            worker_type: One of "mt5_live", "mt5_history", "mt5_ephemeral".
            job_id: The job ID (ephemeral workers only).
            agent_name: The agent name (ephemeral workers only).
            started_at: Unix timestamp of worker start.
            terminal_id: The terminal ID this worker belongs to. Used to notify
                ephemeral event waiters.
        """
        import psutil
        try:
            created = psutil.Process(pid).create_time()
        except psutil.Error:
            created = None
        async with self._lock:
            self._registry[worker_id] = {
                "pid":         pid,
                "process_created": created,
                "port":        port,
                "mql5_source": mql5_source or "",
                "status":      "running",
                "worker_type": worker_type,
                "job_id":      job_id,
                "agent_name":  agent_name,
                "started_at":  started_at or time.time(),
            }
        logger.info(
            f"Worker registered: {worker_id!r} pid={pid} port={port} "
            f"source={mql5_source} type={worker_type}"
        )
        if worker_type == "mt5_ephemeral" and terminal_id and job_id:
            self.notify_ephemeral(terminal_id, job_id)

    # ------------------------------------------------------------------
    # Registry lookups
    # ------------------------------------------------------------------

    async def get_port(self, worker_id: str) -> Optional[int]:
        """Return the HTTP port for a worker by its worker_id, or None if not registered."""
        async with self._lock:
            entry = self._registry.get(worker_id)
            return entry["port"] if entry else None

    def get_port_sync(self, worker_id: str) -> int | None:
        """Synchronous version of get_port() for use from sync contexts.

        Safe to call from the same process since dict reads are atomic in CPython.
        Only use when you can't be in an async context (e.g., from threading code).
        """
        entry = self._registry.get(worker_id)
        return entry.get("port") if entry else None

    async def get_port_by_source(self, source: str, mode: str = "live") -> Optional[int]:
        """Return the HTTP port for the worker serving ``source`` in the given mode.

        mode: "live" → mt5_live, "history" → mt5_history, "ephemeral" → any ephemeral for that source
        Falls back to first available worker of that type if source doesn't match exactly.
        """
        worker_type = f"mt5_{mode}"
        async with self._lock:
            # Exact match first
            for entry in self._registry.values():
                if entry.get("worker_type") == worker_type and entry.get("mql5_source") == source:
                    return entry.get("port")
            # Fallback: first worker of this type regardless of source
            for entry in self._registry.values():
                if entry.get("worker_type") == worker_type:
                    return entry.get("port")
            return None

    def get_port_by_source_sync(self, mql5_source: str, mode: str = "live") -> int | None:
        """Synchronous version of get_port_by_source() for use from sync contexts."""
        worker_type = f"mt5_{mode}"
        for entry in self._registry.values():
            if entry.get("worker_type") == worker_type and entry.get("mql5_source") == mql5_source:
                return entry.get("port")
        for entry in self._registry.values():
            if entry.get("worker_type") == worker_type:
                return entry.get("port")
        return None

    async def get_pid(self, worker_id: str) -> Optional[int]:
        """Return the PID for a worker, or None if not registered."""
        async with self._lock:
            entry = self._registry.get(worker_id)
            return entry["pid"] if entry else None

    async def is_alive(self, worker_id: str) -> bool:
        """Reject dead/reused PIDs and explicitly stopped terminal registrations."""
        async with self._lock:
            entry = self._registry.get(worker_id)
            if not entry:
                return False
            pid = entry["pid"]
        terminal_id = worker_id.partition(':')[2]
        if worker_id.startswith(('mt5_live:', 'mt5_history:')):
            terminal = self.get_terminal(terminal_id)
            if terminal is None or terminal.get('active') is not True:
                return False
        import psutil
        try:
            return (entry.get('process_created') is not None
                    and psutil.Process(pid).create_time() == entry['process_created']
                    and psutil.Process(pid).is_running())
        except psutil.Error:
            return False

    async def clear(self, worker_id: str) -> None:
        """Remove a worker from the registry (call after confirmed death)."""
        async with self._lock:
            self._registry.pop(worker_id, None)
        logger.debug(f"Worker cleared from registry: {worker_id!r}")

    async def clear_ephemeral(self, job_id: str) -> None:
        """Remove all ephemeral entries for a given job_id."""
        async with self._lock:
            to_remove = [
                wid for wid, entry in self._registry.items()
                if entry.get("worker_type") == "mt5_ephemeral" and entry.get("job_id") == job_id
            ]
            for wid in to_remove:
                del self._registry[wid]
        if to_remove:
            logger.debug(f"Cleared ephemeral workers for job {job_id!r}: {to_remove}")

    async def get_by_source(self, mql5_source: str) -> Optional[dict]:
        """Return the first registry entry whose mql5_source matches, or None.

        Prefers live workers for backward compatibility with existing callers.
        """
        async with self._lock:
            # Prefer live worker
            for entry in self._registry.values():
                if entry.get("mql5_source") == mql5_source and entry.get("worker_type") == "mt5_live":
                    return dict(entry)
            # Fall back to any matching source
            for entry in self._registry.values():
                if entry.get("mql5_source") == mql5_source:
                    return dict(entry)
            return None

    async def get_ephemeral_workers(self, source: Optional[str] = None) -> list[dict]:
        """Return all ephemeral worker entries, optionally filtered by mql5_source."""
        async with self._lock:
            result = []
            for wid, entry in self._registry.items():
                if entry.get("worker_type") != "mt5_ephemeral":
                    continue
                if source and entry.get("mql5_source") != source:
                    continue
                result.append({"worker_id": wid, **entry})
            return result

    async def all_workers(self) -> dict:
        """Return a snapshot of the full registry (for diagnostics)."""
        async with self._lock:
            return dict(self._registry)

    def all_workers_sync(self) -> dict:
        """Synchronous version of all_workers() for use from sync contexts."""
        return dict(self._registry)

    # ------------------------------------------------------------------
    # Terminal status
    # ------------------------------------------------------------------

    async def update_terminal_status(self, terminal_id: str, status: dict) -> None:
        """Store the latest status dict from a terminal worker.

        Args:
            terminal_id: The terminal identifier (e.g. "term_abc123").
            status: The status dict reported by the worker (phase, broker,
                connected, mql5_source, utc_offset, ticks, last_update, errors).
        """
        async with self._lock:
            self._terminal_status[terminal_id] = status

    def get_terminal_status(self, terminal_id: str) -> dict | None:
        """Return the cached status for a terminal, or None if not available."""
        return self._terminal_status.get(terminal_id)

    # ------------------------------------------------------------------
    # Config cache
    # ------------------------------------------------------------------

    def _load_config_from_disk(self) -> dict | None:
        """Read config.json from disk and update the cache."""
        if not self._config_path.exists():
            self._terminal_config = None
            self._config_mtime = 0.0
            return None
        try:
            self._config_mtime = self._config_path.stat().st_mtime
            self._terminal_config = json.loads(self._config_path.read_text(encoding="utf-8"))
            return self._terminal_config
        except Exception:
            self._terminal_config = None
            self._config_mtime = 0.0
            return None

    def _ensure_config_fresh(self) -> dict | None:
        """Reload config if the file has changed or cache is empty."""
        if self._terminal_config is None:
            return self._load_config_from_disk()
        if not self._config_path.exists():
            self._terminal_config = None
            self._config_mtime = 0.0
            return None
        try:
            current_mtime = self._config_path.stat().st_mtime
        except OSError:
            self._terminal_config = None
            self._config_mtime = 0.0
            return None
        if current_mtime != self._config_mtime:
            return self._load_config_from_disk()
        return self._terminal_config

    def get_terminals(self) -> list[dict]:
        """Return the terminals list from cached config.

        Reloads from disk if the cache is missing or the file's mtime has changed.
        Returns an empty list if the config file doesn't exist.
        """
        cfg = self._ensure_config_fresh()
        if cfg is None:
            return []
        return cfg.get("terminals", [])

    def get_terminal(self, terminal_id: str) -> dict | None:
        """Return a single terminal config entry by ID, or None if not found."""
        for term in self.get_terminals():
            if term.get("id") == terminal_id:
                return term
        return None

    def get_active_terminal_ids(self) -> list[str]:
        """Return IDs of terminals where ``active`` is True."""
        return [t["id"] for t in self.get_terminals() if t.get("active")]

    def reload_config(self) -> None:
        """Force reload config from disk (call after ``_save_config`` in mt5_manager.py)."""
        self._load_config_from_disk()

    # ------------------------------------------------------------------
    # Ephemeral event tracking
    # ------------------------------------------------------------------

    @staticmethod
    def _ephemeral_key(terminal_id: str, job_id: str) -> str:
        """Build the lookup key for an ephemeral event."""
        return f"{terminal_id}:{job_id}"

    def set_ephemeral_event(self, terminal_id: str, job_id: str, event: threading.Event | None) -> None:
        """Register or clear an ephemeral event.

        Args:
            terminal_id: The terminal identifier.
            job_id: The job identifier.
            event: A ``threading.Event`` to register, or None to clear.
        """
        key = self._ephemeral_key(terminal_id, job_id)
        if event is None:
            self._ephemeral_events.pop(key, None)
        else:
            self._ephemeral_events[key] = event

    def notify_ephemeral(self, terminal_id: str, job_id: str) -> None:
        """Signal the ephemeral event for the given terminal and job, if registered."""
        key = self._ephemeral_key(terminal_id, job_id)
        event = self._ephemeral_events.pop(key, None)
        if event is not None:
            event.set()
            logger.debug(f"Ephemeral event notified: {key!r}")

    def get_all_terminal_info(self) -> dict:
        """Return a combined snapshot of config, status, and worker info for all terminals.

        Returns a dict with a ``"terminals"`` key containing a list of dicts with
        keys: id, label, terminal_path, active, live, history, status.
        """
        terminals = self.get_terminals()
        result = []
        for term in terminals:
            tid = term["id"]
            live_entry = self._registry.get(f"mt5_live:{tid}")
            history_entry = self._registry.get(f"mt5_history:{tid}")
            status = self._terminal_status.get(tid, {})

            live_info = {
                "pid": live_entry["pid"] if live_entry else None,
                "port": live_entry["port"] if live_entry else None,
                "alive": self._check_alive_sync(live_entry["pid"]) if live_entry else False,
            }
            history_info = {
                "pid": history_entry["pid"] if history_entry else None,
                "port": history_entry["port"] if history_entry else None,
                "alive": self._check_alive_sync(history_entry["pid"]) if history_entry else False,
            }

            result.append({
                "id": tid,
                "label": term.get("label", ""),
                "terminal_path": term.get("terminal_path", ""),
                "active": term.get("active", False),
                "live": live_info,
                "history": history_info,
                "status": status,
            })
        return {"terminals": result}

    @staticmethod
    def _check_alive_sync(pid: int | None) -> bool:
        """Synchronous process-alive check (no lock needed)."""
        if pid is None:
            return False
        try:
            import psutil
            return psutil.pid_exists(pid)
        except ImportError:
            pass
        try:
            os.kill(pid, 0)
            return True
        except (OSError, ProcessLookupError):
            return False


# ------------------------------------------------------------------
# Module-level singleton and backward-compatible wrappers
# ------------------------------------------------------------------

_instance = WorkerRegistry()

# Expose singleton as worker_registry for direct instance access
worker_registry = _instance


async def register(
    worker_id: str,
    pid: int,
    port: Optional[int] = None,
    mql5_source: Optional[str] = None,
    worker_type: Optional[str] = None,
    job_id: Optional[str] = None,
    agent_name: Optional[str] = None,
    started_at: Optional[float] = None,
    terminal_id: Optional[str] = None,
) -> None:
    return await _instance.register(
        worker_id, pid, port, mql5_source, worker_type, job_id, agent_name, started_at, terminal_id
    )


async def get_port(worker_id: str) -> Optional[int]:
    return await _instance.get_port(worker_id)


def get_port_sync(worker_id: str) -> int | None:
    return _instance.get_port_sync(worker_id)


async def get_port_by_source(source: str, mode: str = "live") -> Optional[int]:
    return await _instance.get_port_by_source(source, mode)


def get_port_by_source_sync(mql5_source: str, mode: str = "live") -> int | None:
    return _instance.get_port_by_source_sync(mql5_source, mode)


async def get_pid(worker_id: str) -> Optional[int]:
    return await _instance.get_pid(worker_id)


async def is_alive(worker_id: str) -> bool:
    return await _instance.is_alive(worker_id)


async def clear(worker_id: str) -> None:
    return await _instance.clear(worker_id)


async def clear_ephemeral(job_id: str) -> None:
    return await _instance.clear_ephemeral(job_id)


async def get_by_source(mql5_source: str) -> Optional[dict]:
    return await _instance.get_by_source(mql5_source)


async def get_ephemeral_workers(source: Optional[str] = None) -> list[dict]:
    return await _instance.get_ephemeral_workers(source)


async def all_workers() -> dict:
    return await _instance.all_workers()


def all_workers_sync() -> dict:
    return _instance.all_workers_sync()


async def update_terminal_status(terminal_id: str, status: dict) -> None:
    return await _instance.update_terminal_status(terminal_id, status)


def get_terminal_status(terminal_id: str) -> dict | None:
    return _instance.get_terminal_status(terminal_id)


def get_terminals() -> list[dict]:
    return _instance.get_terminals()


def get_terminal(terminal_id: str) -> dict | None:
    return _instance.get_terminal(terminal_id)


def get_active_terminal_ids() -> list[str]:
    return _instance.get_active_terminal_ids()


def reload_config() -> None:
    _instance.reload_config()


def set_ephemeral_event(terminal_id: str, job_id: str, event: threading.Event | None) -> None:
    _instance.set_ephemeral_event(terminal_id, job_id, event)


def notify_ephemeral(terminal_id: str, job_id: str) -> None:
    _instance.notify_ephemeral(terminal_id, job_id)


def get_all_terminal_info() -> dict:
    return _instance.get_all_terminal_info()
