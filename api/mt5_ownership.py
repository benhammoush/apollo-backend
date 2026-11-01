"""Fixed local-host MT5 ownership adapter. Never auto-activated by importing it."""

from dataclasses import dataclass
from pathlib import Path
import os
import sys

from api.mt5_binding import MT5PairOwnership, target_revision
from shared.mt5_ownership import operation_fence, worker_fence, OwnershipBusy


@dataclass(frozen=True)
class WorkerProcess:
    """Exact executable/script identity plus creation time, not a reusable PID."""
    pid: int
    created: float
    mode: str


def inspect_processes(root: Path, terminal_id: str) -> list[WorkerProcess]:
    """Include pending/unregistered children; ambiguity is unavailable, not absent.

    Inaccessible process metadata intentionally blocks startup. This host must
    provide complete process visibility before public startup can be activated.
    No account/config/status files are read here.
    """
    import psutil
    scripts = {os.path.normcase(str((Path(root) / relative).resolve()))
               for relative in ('workers/mt5/run_mt5_worker.py', 'live/run_mt5_worker.py')}
    found = []
    identities = {}
    for process in psutil.process_iter():
        try:
            name = process.name().lower()
            if "python" not in name:
                continue
            args = process.cmdline()
            if not args:
                raise ValueError("Incomplete process inventory")
            candidates = [arg for arg in args[1:] if arg.lower().endswith("run_mt5_worker.py")]
            if not candidates:
                continue
            if len(candidates) != 1 or not Path(candidates[0]).is_absolute():
                raise ValueError("Ambiguous MT5 process identity")
            if os.path.normcase(str(Path(candidates[0]).resolve())) not in scripts:
                continue
            if args.count("--terminal-id") != 1 or args.count("--mode") != 1:
                raise ValueError("Ambiguous MT5 command line")
            tid = args[args.index("--terminal-id") + 1]
            mode = args[args.index("--mode") + 1]
            if tid == terminal_id:
                if mode not in ("live", "history", "ephemeral"):
                    raise ValueError("Unknown worker mode")
                if mode != "ephemeral":
                    created = process.create_time()
                    identities[process.pid] = (process.ppid(), os.path.normcase(str(Path(process.exe()).resolve())), tuple(args[1:]))
                    found.append(WorkerProcess(process.pid, created, mode))
        except psutil.NoSuchProcess:
            continue
        except (psutil.AccessDenied, psutil.ZombieProcess, IndexError, OSError):
            raise ValueError("MT5 process inventory unavailable") from None
    # Windows venv python.exe is a launcher which waits for a base-interpreter
    # child with the same script arguments. These are one worker, not duplicates.
    # Never collapse arbitrary parent/child processes or repeated mode names.
    if os.name == "nt":
        launcher_dir = Path(sys.executable).resolve().parent
        base_dir = Path(sys._base_executable).resolve().parent
        launchers = {os.path.normcase(str(launcher_dir / name)): os.path.normcase(str(base_dir / name))
                     for name in ('python.exe', 'pythonw.exe')}
        if launcher_dir != base_dir:
            wrappers = set()
            for child in found:
                parent_pid, executable, arguments = identities[child.pid]
                parent = identities.get(parent_pid)
                if (parent is not None and launchers.get(parent[1]) == executable
                        and parent[2] == arguments
                        and sum(item[0] == parent_pid for item in identities.values()) == 1):
                    wrappers.add(parent_pid)
            found = [entry for entry in found if entry.pid not in wrappers]
    return found


def pair_state(root: Path, terminal_id: str, processes: list[WorkerProcess]) -> str:
    """Only a complete empty inventory AND free lifetime fences prove absence."""
    counts = [sum(p.mode == mode for p in processes) for mode in ("live", "history")]
    if max(counts) > 1:
        return "unavailable"
    if processes:
        return "running" if counts == [1, 1] else "partial"
    held = []
    try:
        for mode in ("live", "history"):
            try:
                held.append(worker_fence(root, terminal_id, mode).acquire())
            except OwnershipBusy:
                return "starting"
        return "stopped"
    finally:
        for fence in held:
            fence.release()


class LocalMT5PairOwnership(MT5PairOwnership):
    """Use the same operation fence and launcher as private mutations/recovery.

    Construction alone is not activation approval. Source/pin approval and isolated
    real-host validation remain required by bind_market_bootstrap().
    """

    def __init__(self, root: Path):
        self.root = Path(root).resolve()

    def _validate(self, target):
        """Recheck the pin inside the shared operation fence."""
        revision, _ = target_revision(self.root / "cache/mt5/config.json", target.terminal_id, require_active=True)
        if revision != target.config_revision:
            raise ValueError("Approved target changed")

    async def inspect_pair(self, target):
        """Return only fixed-target state; a busy operation cannot advertise start."""
        with operation_fence(self.root):
            self._validate(target)
            return pair_state(self.root, target.terminal_id, inspect_processes(self.root, target.terminal_id))

    async def start_if_absent(self, target):
        """Recheck and launch synchronously under the same cross-process fence."""
        from api.mt5_host import MT5Host
        with operation_fence(self.root):
            self._validate(target)
            if pair_state(self.root, target.terminal_id, inspect_processes(self.root, target.terminal_id)) != "stopped":
                raise OwnershipBusy("MT5 pair is no longer absent")
            MT5Host(self.root).start_pair(target.terminal_id, expected_source=target.expected_source)
