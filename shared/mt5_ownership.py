"""Local-host MT5 operation fencing; locks are kernel-owned, never PID leases.

Lock files must never be deleted while a host is in use. All cooperating API
processes and children use the same absolute application root and lock files.
This is not a distributed/network-filesystem lock or an authentication boundary.
"""

from __future__ import annotations

import asyncio
from functools import wraps
import os
from pathlib import Path
import re
import time


class OwnershipBusy(RuntimeError):
    """Another host operation or worker owns the requested fence."""


class FileFence:
    """Non-inheritable, nonblocking exclusive byte lock, released on process exit."""

    def __init__(self, path: Path):
        self.path = Path(path)
        self.stream = None

    def acquire(self):
        """Acquire without blocking the event loop or stealing an expired lease."""
        if self.stream is not None:
            raise OwnershipBusy("Fence already acquired")
        self.path.parent.mkdir(parents=True, exist_ok=True)
        stream = self.path.open("a+b")
        os.set_inheritable(stream.fileno(), False)
        try:
            if stream.seek(0, 2) == 0:
                stream.write(b"0")
                stream.flush()
            stream.seek(0)
            if os.name == "nt":
                import msvcrt
                msvcrt.locking(stream.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            stream.close()
            raise OwnershipBusy("MT5 ownership fence unavailable") from None
        self.stream = stream
        return self

    def release(self):
        """Unlock Windows' first byte before close; never unlink or steal a lock."""
        if self.stream is not None:
            stream = self.stream
            self.stream = None
            try:
                if os.name == "nt":
                    import msvcrt
                    stream.seek(0)
                    msvcrt.locking(stream.fileno(), msvcrt.LK_UNLCK, 1)
            finally:
                stream.close()

    def __enter__(self):
        return self.acquire()

    def __exit__(self, *_):
        self.release()


def operation_fence(root: Path) -> FileFence:
    """Serialize configuration read-modify-write and all pair lifecycle operations."""
    return FileFence(Path(root) / "cache/mt5/ownership/operations.lock")


def wait_for_startup_fence(root: Path, *, timeout: float = 90) -> FileFence:
    """Child-only bounded wait so live/history can initialize sequentially.

    Host request handlers never use this blocking wait. A waiting child remains
    visible in the process inventory and owns its mode fence throughout.
    """
    deadline = time.monotonic() + timeout
    while True:
        try:
            return operation_fence(root).acquire()
        except OwnershipBusy:
            if time.monotonic() >= deadline:
                raise
            time.sleep(0.05)


def worker_fence(root: Path, terminal_id: str, mode: str) -> FileFence:
    """Lifetime fence prevents duplicate live/history workers before registration."""
    if re.fullmatch(r"term_[0-9a-f]{8}", terminal_id) is None or mode not in ("live", "history"):
        raise ValueError("Invalid MT5 worker identity")
    return FileFence(Path(root) / f"cache/mt5/ownership/{terminal_id}.{mode}.lock")


def serialized_operation(root: Path):
    """Fence an entire async mutation; caller cancellation cannot release it early.

    Contention fails closed instead of blocking the event loop. Cancellation waits
    for the bounded operation to finish, including partial launch/stop bookkeeping.
    """
    def decorate(function):
        @wraps(function)
        async def wrapped(*args, **kwargs):
            async def run():
                with operation_fence(root):
                    return await function(*args, **kwargs)
            task = asyncio.create_task(run())
            cancelled = False
            while True:
                try:
                    result = await asyncio.shield(task)
                    break
                except asyncio.CancelledError:
                    if task.cancelled():
                        raise
                    cancelled = True
                except BaseException:
                    if cancelled:
                        raise asyncio.CancelledError from None
                    raise
            if cancelled:
                raise asyncio.CancelledError
            return result
        return wrapped
    return decorate
