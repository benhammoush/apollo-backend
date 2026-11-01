"""Independent local-host controls with synthetic configs; no native MT5 or listeners."""

import asyncio
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch

from api.mt5_host import MT5Host
from api.mt5_binding import target_revision
from local_workers import control, host_fence, load_targets
from run_local import serve_local
from api.worker_registry import WorkerRegistry
from api.live_runtime import WorkerRegisterBody, internal_worker_register


class LocalWorkerTests(unittest.IsolatedAsyncioTestCase):
    """Exercise persisted intent, explicit selection and shared lifecycle primitives."""

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.tid = 'term_1234abcd'
        self.config = self.root / 'cache/mt5/config.json'
        self.config.parent.mkdir(parents=True)
        self.config.write_text(json.dumps({'terminals': [
            {'id': self.tid, 'terminal_path': 'synthetic-terminal.exe', 'active': False}]}))
        self.profile = self.root / 'config.local.json'
        self.profile.write_text(json.dumps({'targets': [{'terminal_id': self.tid, 'source_alias': 'market'}]}))
        self.policy = SimpleNamespace(sources={'market': 'synthetic-source'}, symbols=('EURUSD',))
        self.targets = {self.tid: 'synthetic-source'}
        self.host = MT5Host(self.root, inspector=lambda *_: [], spawn=lambda *_a, **_kw: 1234,
                            symbols=self.policy.symbols)

    def test_exact_targets_only_and_unknown_alias_denied(self):
        self.assertEqual(load_targets(self.root, self.policy), self.targets)
        self.profile.write_text(json.dumps({'targets': [{'terminal_id': self.tid, 'source_alias': 'unknown'}]}))
        with self.assertRaises(ValueError):
            load_targets(self.root, self.policy)

    async def test_start_requires_host_and_keeps_inactive_on_failure(self):
        with self.assertRaises(ValueError):
            await control(self.root, 'start', self.tid, self.targets, host=self.host)
        self.assertFalse(target_revision(self.config, self.tid)[1])

    async def test_start_pins_source_and_selected_symbols_on_both_workers(self):
        calls = []
        self.host.spawn = lambda script, args, **kw: calls.append((script, args)) or 1234
        with host_fence(self.root):
            result = await control(self.root, 'start', self.tid, self.targets, host=self.host)
        self.assertTrue(result['active'])
        self.assertEqual(len(calls), 2)
        for script, args in calls:
            self.assertEqual(script, 'live/run_mt5_worker.py')
            self.assertEqual(args[args.index('--expected-source') + 1], 'synthetic-source')
            self.assertEqual(args[args.index('--symbols') + 1], 'EURUSD')

    async def test_stop_persists_inactive_even_when_kill_fails(self):
        data = json.loads(self.config.read_text())
        data['terminals'][0]['active'] = True
        self.config.write_text(json.dumps(data))
        self.host.stop_pair = AsyncMock(side_effect=ValueError('synthetic kill failure'))
        with self.assertRaises(ValueError):
            await control(self.root, 'stop', self.tid, self.targets, host=self.host)
        self.assertFalse(target_revision(self.config, self.tid)[1])

    async def test_unknown_terminal_never_mutates_config(self):
        before = self.config.read_bytes()
        with self.assertRaises(ValueError):
            await control(self.root, 'start', 'term_deadbeef', self.targets, host=self.host)
        self.assertEqual(before, self.config.read_bytes())

    async def test_cancelled_stop_keeps_fence_until_kill_finishes(self):
        entered, finish = asyncio.Event(), asyncio.Event()
        async def stop(*_):
            entered.set()
            await finish.wait()
        self.host.stop_pair = stop
        task = asyncio.create_task(control(self.root, 'stop', self.tid, self.targets, host=self.host))
        await entered.wait()
        task.cancel()
        await asyncio.sleep(0)
        self.assertFalse(task.done())
        finish.set()
        with self.assertRaises(asyncio.CancelledError):
            await task

    async def test_host_selects_one_runtime_and_does_not_start_workers(self):
        with patch('run_local.ROOT', self.root), patch('run_local.serve_live', new=AsyncMock()) as serve:
            await serve_local(self.policy, self.targets)
        self.assertEqual(serve.await_args.kwargs['recovery_terminal_ids'], (self.tid,))
        self.assertEqual(serve.await_args.kwargs['mt5_host'].symbols, ('EURUSD',))
        self.assertFalse(target_revision(self.config, self.tid)[1])

    async def test_stopped_intent_and_pid_reuse_are_dead_across_registries(self):
        registry = WorkerRegistry()
        registry._config_path = self.config
        process = SimpleNamespace(create_time=lambda: 10, is_running=lambda: True)
        with patch('psutil.Process', return_value=process):
            await registry.register(f'mt5_live:{self.tid}', 1234, port=9999)
            # Separate CLI registry: stop intent must gate reads without its clear().
            self.assertFalse(await registry.is_alive(f'mt5_live:{self.tid}'))
            data = json.loads(self.config.read_text())
            data['terminals'][0]['active'] = True
            self.config.write_text(json.dumps(data))
            registry.reload_config()
            self.assertTrue(await registry.is_alive(f'mt5_live:{self.tid}'))
            process.create_time = lambda: 11
            self.assertFalse(await registry.is_alive(f'mt5_live:{self.tid}'))

    async def test_surviving_worker_reregisters_into_replacement_registry(self):
        from local_workers import save_intent
        save_intent(self.host, self.tid, True)
        self.host.inspect = lambda *_: [SimpleNamespace(pid=1234, mode='history')]
        state = SimpleNamespace(market_sources={'synthetic-source'}, mt5_host=self.host,
                                recovery_terminal_ids={self.tid})
        request = SimpleNamespace(app=SimpleNamespace(state=state))
        body = WorkerRegisterBody(worker_id=f'mt5_history:{self.tid}', pid=1234, port=9999,
            mql5_source='synthetic-source', worker_type='mt5_history', terminal_id=self.tid)
        replacement = WorkerRegistry()
        replacement._config_path = self.config
        process = SimpleNamespace(create_time=lambda: 10, is_running=lambda: True)
        with patch('psutil.Process', return_value=process), \
                patch('api.live_runtime.worker_registry', replacement), \
                patch('api.live_runtime.ws_manager.broadcast', new=AsyncMock()):
            await internal_worker_register(body, request)
            self.assertEqual(await replacement.get_port(body.worker_id), 9999)
            self.assertTrue(await replacement.is_alive(body.worker_id))

    async def test_registration_loop_retries_without_stopping_surviving_worker(self):
        from live import run_mt5_worker as worker
        sleeps = []
        async def sleep(_):
            sleeps.append(True)
            if len(sleeps) == 2:
                worker._running = False
        with patch.object(worker, '_running', True), \
                patch.object(worker.asyncio, 'sleep', new=AsyncMock(side_effect=sleep)), \
                patch.object(worker, '_register_with_api', new=AsyncMock()) as register:
            await worker._registration_loop(9999, 'synthetic-source')
            register.assert_awaited_once_with(9999, 'synthetic-source', retries=1, stop_on_failure=False)


if __name__ == '__main__':
    unittest.main()
