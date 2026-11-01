"""Trusted local MT5 process primitives, separate from operator HTTP composition.

Callers own the shared operation fence around lifecycle operations. No source,
account, entrypoint or command arguments are selected by visitor requests.
Construction/import does not inspect configuration or create runtime directories.
"""

import asyncio
from datetime import datetime
import json
from pathlib import Path
import subprocess
import sys

from api.mt5_binding import target_revision
from api import mt5_ownership
from shared.mt5_ownership import worker_fence

PUBLIC_WORKER = 'live/run_mt5_worker.py'
PRIVATE_WORKER = 'workers/mt5/run_mt5_worker.py'


class WorkerLaunchError(RuntimeError):
    """A trusted host failed to create a worker process."""


class MT5Host:
    """Share fixed-pair launch/recovery/stop semantics without importing a router."""

    def __init__(self, root: Path, *, config_path=None, worker_entrypoint=PUBLIC_WORKER,
                   inspector=None, spawn=None, symbols=None, runtime_activation=None):
        """Bind server-owned paths/profile and optional internal test collaborators."""
        if worker_entrypoint not in {PUBLIC_WORKER, PRIVATE_WORKER}:
            raise ValueError('Unknown MT5 worker profile')
        self.root = Path(root).resolve()
        self.config_path = Path(config_path) if config_path is not None else self.root / 'cache/mt5/config.json'
        self.worker_entrypoint = worker_entrypoint
        self.inspect = inspector if inspector is not None else mt5_ownership.inspect_processes
        self.spawn = spawn if spawn is not None else self.spawn_script
        self.runtime_managed = spawn is None
        self.runtime_activation = runtime_activation
        self.symbols = tuple(symbols) if symbols is not None else ()
        if any(type(symbol) is not str or not symbol.replace('_', '').isalnum() for symbol in self.symbols):
            raise ValueError('Invalid local worker symbols')

    def spawn_script(self, script, extra_args=(), log_file=None):
        """Internal compatibility primitive; pair launches always use a fixed entrypoint."""
        owned_log = None
        if log_file is None:
            directory = self.root / 'cache/logs'
            directory.mkdir(parents=True, exist_ok=True)
            stem = str(script).replace('.py', '').replace('/', '_').replace('\\', '_')
            name = stem + '_' + '_'.join(extra_args).replace('--', '').replace(' ', '_') + '.log'
            owned_log = log_file = (directory / name).open('a', encoding='utf-8')
        kwargs = {'stdout': log_file, 'stderr': log_file, 'cwd': self.root}
        if sys.platform == 'win32':
            kwargs['creationflags'] = subprocess.CREATE_NO_WINDOW
        try:
            return subprocess.Popen([sys.executable, '-E', '-s', '-B', str(self.root / script), *extra_args], **kwargs).pid
        except Exception as error:
            raise WorkerLaunchError(str(error)) from error
        finally:
            if owned_log is not None:
                owned_log.close()

    def approved_source_pin(self, terminal_id, revision, expected_source=None):
        """Retain exact public source/config approval across private recovery/restarts."""
        path = self.config_path.parent / 'ownership' / f'{terminal_id}.approval.json'
        if path.exists():
            with path.open('rb') as stream:
                raw = stream.read(4097)
            if len(raw) > 4096:
                raise ValueError('Invalid MT5 source pin')
            pin = json.loads(raw)
            if (type(pin) is not dict or set(pin) != {'revision', 'source'} or pin['revision'] != revision
                    or type(pin['source']) is not str or not pin['source'].strip()
                    or (expected_source is not None and pin['source'] != expected_source)):
                raise ValueError('MT5 source pin requires private review')
            return pin['source']
        if expected_source is not None:
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open('x', encoding='utf-8') as stream:
                json.dump({'revision': revision, 'source': expected_source}, stream)
        return expected_source

    def start_pair(self, terminal_id, *, expected_source=None):
        """Caller holds operation fence; validate and fill only genuinely absent modes."""
        if self.runtime_managed:
            from api.runtime_session import assert_local_launch
            assert_local_launch(self.root, terminal_id, self.runtime_activation)
        revision, _ = target_revision(self.config_path, terminal_id, require_active=True)
        expected_source = self.approved_source_pin(terminal_id, revision, expected_source)
        by_mode = {}
        for entry in self.inspect(self.root, terminal_id):
            if entry.mode in by_mode:
                raise ValueError('Duplicate MT5 workers require private recovery')
            by_mode[entry.mode] = entry.pid
        extra = ['--config-revision', revision]
        if self.runtime_managed:
            extra += ['--runtime-activation', self.runtime_activation]
        if self.symbols:
            extra += ['--symbols', *self.symbols]
        if expected_source is not None:
            extra += ['--expected-source', expected_source]
        date = datetime.now().strftime('%Y-%m-%d')
        for mode in ('live', 'history'):
            directory = self.root / 'cache/logs/mt5' / mode
            directory.mkdir(parents=True, exist_ok=True)
            if mode not in by_mode:
                with (directory / f'{terminal_id}_{date}.log').open('a', encoding='utf-8') as log:
                    by_mode[mode] = self.spawn(self.worker_entrypoint,
                        ['--terminal-id', terminal_id, '--mode', mode, *extra], log_file=log)
        return {'live_pid': by_mode['live'], 'history_pid': by_mode['history']}

    async def stop_pair(self, terminal_id, registry):
        """Stop exact identities and rescan before clearing the caller's shared registry."""
        import psutil
        for _ in range(4):
            children = self.inspect(self.root, terminal_id)
            if not children:
                for mode in ('live', 'history'):
                    with worker_fence(self.root, terminal_id, mode):
                        pass
                break
            for entry in children:
                try:
                    process = psutil.Process(entry.pid)
                    if process.create_time() != entry.created:
                        raise ValueError('MT5 PID identity changed')
                    process.kill()
                    for _ in range(10):
                        if not process.is_running() or process.status() == psutil.STATUS_ZOMBIE:
                            break
                        await asyncio.sleep(0.5)
                    else:
                        raise ValueError('MT5 child did not stop')
                except psutil.NoSuchProcess:
                    pass
        else:
            raise ValueError('MT5 descendants remain after bounded stop')
        for mode in ('live', 'history'):
            await registry.clear(f'mt5_{mode}:{terminal_id}')
