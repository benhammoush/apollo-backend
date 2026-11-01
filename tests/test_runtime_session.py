"""Offline task/session lifecycle checks; all native sessions, dispatch and spawning are synthetic."""

import asyncio
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch

from api import runtime_session as runtime
from api.mt5_host import MT5Host
from local_workers import host_fence, save_intent, control


class RuntimeSessionTests(unittest.IsolatedAsyncioTestCase):
    """Exercise real file fencing/intent with mocked Windows identity and task execution."""

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.tid = 'term_1234abcd'
        self.config = self.root / 'cache/mt5/config.json'
        self.config.parent.mkdir(parents=True)
        self.config.write_text(json.dumps({'terminals': [{'id': self.tid,
            'terminal_path': 'synthetic.exe', 'active': False}]}))
        (self.root / 'config.local.json').write_text(json.dumps({'targets': [
            {'terminal_id': self.tid, 'source_alias': 'market'}]}))
        self.policy = SimpleNamespace(sources={'market': 'synthetic-source'}, symbols=('EURUSD',))
        self.binding = {'sid': 'synthetic-sid', 'session_id': 37, 'logon': 123456}
        self.calls = []
        self.host = MT5Host(self.root, inspector=lambda *_: [],
            spawn=lambda script, args, **kw: self.calls.append((script, args)) or 1234,
            symbols=self.policy.symbols)
        for name, replacement in [
            ('policy', lambda _: {'sid': 'synthetic-sid'}),
            ('inspect_bound', lambda host, tid, binding: host.inspect(host.root, tid)),
        ]:
            context = patch.object(runtime, name, replacement)
            context.start()
            self.addCleanup(context.stop)
        context = patch.object(runtime.windows, 'selected_session', return_value=self.binding)
        self.selected = context.start()
        self.addCleanup(context.stop)
        context = patch.object(runtime.windows, 'assert_current')
        self.current = context.start()
        self.addCleanup(context.stop)

    def prepare(self):
        """Stage an explicit synthetic activation while simulating an owning API host."""
        with host_fence(self.root):
            return runtime._request(self.host, self.tid, 'synthetic-source', explicit=True)

    def run_task(self):
        """Run the real fixed action with only process creation replaced."""
        with patch('api.mt5_host.MT5Host', return_value=self.host):
            runtime.run_pending(self.root, self.policy)

    def test_api_required_before_intent_changes(self):
        with self.assertRaisesRegex(ValueError, 'API host'):
            runtime._request(self.host, self.tid, 'synthetic-source', explicit=True)
        self.assertFalse(json.loads(self.config.read_text())['terminals'][0]['active'])

    def test_task_only_launches_in_exact_login_and_pins_source(self):
        activation, request, _ = self.prepare()
        self.run_task()
        self.current.assert_called_once_with(self.binding)
        self.assertEqual(len(self.calls), 2)
        state = runtime.read_state(self.root)
        self.assertIsNone(state['pending'])
        self.assertEqual(state['result'], {'id': request, 'ok': True})
        self.assertEqual(state['activation'], activation)
        for _, args in self.calls:
            self.assertEqual(args[args.index('--expected-source') + 1], 'synthetic-source')

    def test_stop_invalidates_queued_task_before_it_can_launch(self):
        self.prepare()
        with runtime.operation_fence(self.root):
            runtime.invalidate(self.root)
            save_intent(self.host, self.tid, False)
        self.run_task()
        self.assertFalse(self.calls)

    def test_reused_session_number_is_not_the_same_login(self):
        self.prepare()
        self.selected.return_value = {**self.binding, 'logon': self.binding['logon'] + 1}
        with self.assertRaisesRegex(ValueError, 'ended login'):
            self.run_task()
        self.assertFalse(self.calls)

    def test_wrong_current_desktop_cannot_run_task(self):
        self.prepare()
        self.current.side_effect = ValueError('wrong desktop')
        with self.assertRaisesRegex(ValueError, 'wrong desktop'):
            self.run_task()
        self.assertFalse(self.calls)

    def test_missing_or_ambiguous_runtime_login_never_enables(self):
        self.selected.side_effect = runtime.windows.SessionError('ambiguous login')
        with self.assertRaisesRegex(ValueError, 'ambiguous'):
            self.prepare()
        self.assertFalse(json.loads(self.config.read_text())['terminals'][0]['active'])

    def test_duplicate_explicit_start_does_not_replace_pending_request(self):
        self.prepare()
        before = runtime.read_state(self.root)
        with self.assertRaises(runtime.OwnershipBusy):
            self.prepare()
        self.assertEqual(runtime.read_state(self.root), before)

    def test_recovery_without_activation_never_launches(self):
        with host_fence(self.root), self.assertRaises(ValueError):
            runtime._request(self.host, self.tid, None, explicit=False)
        self.assertFalse(self.calls)

    def test_same_login_api_replacement_can_recover_both_missing_modes(self):
        self.prepare()
        self.run_task()
        self.calls.clear()
        with host_fence(self.root):
            request = runtime._request(self.host, self.tid, None, explicit=False)
        self.assertIsNotNone(request)
        self.run_task()
        self.assertEqual(len(self.calls), 2)

    def test_recovery_pending_request_is_not_duplicated(self):
        self.prepare()
        with host_fence(self.root):
            self.assertIsNone(runtime._request(self.host, self.tid, None, explicit=False))

    def test_partial_recovery_only_fills_missing_mode(self):
        self.prepare()
        self.run_task()
        self.calls.clear()
        self.host.inspect = lambda *_: [SimpleNamespace(pid=12, mode='live')]
        with host_fence(self.root):
            runtime._request(self.host, self.tid, None, explicit=False)
        self.run_task()
        self.assertEqual(len(self.calls), 1)
        self.assertEqual(self.calls[0][1][self.calls[0][1].index('--mode') + 1], 'history')

    def test_expired_task_records_failure_without_launch(self):
        self.prepare()
        state = runtime.read_state(self.root)
        state['pending']['expires'] = 0
        runtime.write_state(self.root, state)
        with self.assertRaisesRegex(ValueError, 'Expired'):
            self.run_task()
        self.assertFalse(self.calls)
        self.assertFalse(runtime.read_state(self.root)['result']['ok'])

    def test_source_or_terminal_change_blocks_task(self):
        self.prepare()
        data = json.loads(self.config.read_text())
        data['terminals'][0]['terminal_path'] = 'changed.exe'
        self.config.write_text(json.dumps(data))
        with self.assertRaisesRegex(ValueError, 'activation changed'):
            self.run_task()
        self.assertFalse(self.calls)

    def test_corrupt_activation_is_unavailable_not_fresh_start(self):
        runtime.state_path(self.root).parent.mkdir(parents=True)
        runtime.state_path(self.root).write_text('not json')
        with self.assertRaises(ValueError):
            self.prepare()

    def test_direct_native_launcher_without_activation_is_denied(self):
        self.prepare()
        host = MT5Host(self.root)
        with self.assertRaisesRegex(ValueError, 'cancelled or replaced'):
            host.start_pair(self.tid)

    async def test_dispatch_failure_disables_intent_and_late_task(self):
        with host_fence(self.root), patch.object(runtime, 'dispatch', side_effect=ValueError('dispatch failed')):
            with self.assertRaisesRegex(ValueError, 'dispatch failed'):
                await runtime.request_launch(self.host, self.tid, 'synthetic-source', explicit=True)
        self.assertEqual(runtime.read_state(self.root), {'disabled': True})
        self.assertFalse(json.loads(self.config.read_text())['terminals'][0]['active'])
        self.run_task()
        self.assertFalse(self.calls)

    async def test_start_waits_for_task_confirmation_not_com_success(self):
        with host_fence(self.root), patch.object(runtime, 'dispatch', side_effect=lambda *_: self.run_task()):
            await runtime.request_launch(self.host, self.tid, 'synthetic-source', explicit=True)
        self.assertEqual(len(self.calls), 2)

    async def test_stop_works_with_absent_runtime_desktop(self):
        self.prepare()
        self.host.runtime_managed = True
        self.host.stop_pair = AsyncMock()
        self.selected.side_effect = runtime.windows.SessionError('no login')
        result = await control(self.root, 'stop', self.tid, {self.tid: 'synthetic-source'}, host=self.host)
        self.assertFalse(result['active'])
        self.assertEqual(result['state'], 'stopped')
        self.assertEqual(result['session'], 'unavailable')
        self.host.stop_pair.assert_awaited_once()

    async def test_recovery_entrypoint_routes_task_never_api_spawn(self):
        from api.live_runtime import _respawn_terminal_worker
        self.host.runtime_managed = True
        with patch.object(runtime, 'request_launch', new=AsyncMock()) as request:
            await _respawn_terminal_worker(self.tid, host=self.host)
        request.assert_awaited_once_with(self.host, self.tid)
        self.assertFalse(self.calls)

    async def test_api_shutdown_during_recovery_preserves_established_activation(self):
        self.prepare()
        self.run_task()
        activation = runtime.read_state(self.root)['activation']
        entered = asyncio.Event()
        async def dispatch(*_):
            entered.set()
            await asyncio.Event().wait()
        with host_fence(self.root), patch.object(runtime.asyncio, 'to_thread', side_effect=self.inline_thread), \
                patch.object(runtime, 'dispatch', side_effect=dispatch):
            task = asyncio.create_task(runtime.request_launch(self.host, self.tid))
            await entered.wait()
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
        state = runtime.read_state(self.root)
        self.assertEqual(state['activation'], activation)
        self.assertTrue(state['established'])
        self.assertIsNone(state['pending'])
        self.assertTrue(json.loads(self.config.read_text())['terminals'][0]['active'])

    async def inline_thread(self, function, *args, **kwargs):
        """Inline synchronous fixtures while allowing a deliberately suspended async dispatch."""
        value = function(*args, **kwargs)
        return await value if asyncio.iscoroutine(value) else value

    async def test_cancellation_retains_preparation_until_thread_finishes(self):
        import threading
        entered, finish = threading.Event(), threading.Event()
        original = runtime._request
        def prepare(*args, **kwargs):
            entered.set()
            finish.wait(3)
            return original(*args, **kwargs)
        with host_fence(self.root), patch.object(runtime, '_request', side_effect=prepare), \
                patch.object(runtime, 'dispatch') as dispatch:
            task = asyncio.create_task(runtime.request_launch(self.host, self.tid, 'synthetic-source', explicit=True))
            while not entered.is_set():
                await asyncio.sleep(0.01)
            task.cancel()
            await asyncio.sleep(0.02)
            self.assertFalse(task.done())
            finish.set()
            with self.assertRaises(asyncio.CancelledError):
                await task
        dispatch.assert_not_called()
        self.assertEqual(runtime.read_state(self.root), {'disabled': True})
        self.assertFalse(json.loads(self.config.read_text())['terminals'][0]['active'])

    def test_expired_recovery_request_can_be_reconciled_after_api_crash(self):
        self.prepare()
        self.run_task()
        with host_fence(self.root):
            old_request = runtime._request(self.host, self.tid, None, explicit=False)
            state = runtime.read_state(self.root)
            state['pending']['expires'] = 0
            runtime.write_state(self.root, state)
            new_request = runtime._request(self.host, self.tid, None, explicit=False)
        self.assertNotEqual(old_request[1], new_request[1])
        self.assertEqual(old_request[0], new_request[0])

    def test_unconfirmed_expired_start_cannot_be_resumed_by_api(self):
        self.prepare()
        state = runtime.read_state(self.root)
        state['pending']['expires'] = 0
        runtime.write_state(self.root, state)
        with host_fence(self.root), self.assertRaisesRegex(ValueError, 'Unconfirmed Start'):
            runtime._request(self.host, self.tid, None, explicit=False)
        self.assertEqual(runtime.read_state(self.root), {'disabled': True})
        self.assertFalse(json.loads(self.config.read_text())['terminals'][0]['active'])

    async def test_spawn_without_registration_retains_recovery_backoff(self):
        from api import live_runtime
        sleeps = 0
        async def sleep(_):
            nonlocal sleeps
            sleeps += 1
            if sleeps == 2:
                raise asyncio.CancelledError
        self.host.runtime_managed = True
        backoff = {}
        with patch.object(live_runtime, '_runtime_backoff', backoff), \
                patch.object(live_runtime.asyncio, 'sleep', side_effect=sleep), \
                patch.object(live_runtime.worker_registry, 'is_alive', new=AsyncMock(return_value=False)), \
                patch.object(runtime, 'request_launch', new=AsyncMock(return_value=True)) as request:
            with self.assertRaises(asyncio.CancelledError):
                await live_runtime._mt5_watchdog({self.tid}, host=self.host)
        request.assert_awaited_once()
        self.assertEqual(backoff[self.tid][1], 10)


class RuntimeIdentityTests(unittest.TestCase):
    """Guard native launch boundaries and interpreter-wrapper inventory with synthetic processes."""

    def test_process_session_or_owner_mismatch_is_not_a_running_pair(self):
        entry = SimpleNamespace(pid=12, created=10, mode='live')
        host = SimpleNamespace(root=Path('synthetic'), inspect=lambda *_: [entry])
        binding = {'sid': 'runtime', 'session_id': 37, 'logon': 1}
        process = SimpleNamespace(create_time=lambda: 10, username=lambda: 'runtime')
        with patch('psutil.Process', return_value=process), \
                patch.object(runtime.windows, 'account_sid', return_value='runtime'), \
                patch.object(runtime.windows, 'process_session', return_value=37) as session:
            self.assertEqual(runtime.inspect_bound(host, 'term_1234abcd', binding), [entry])
            session.return_value = 2
            with self.assertRaisesRegex(ValueError, 'mismatch'):
                runtime.inspect_bound(host, 'term_1234abcd', binding)
        with patch('psutil.Process', return_value=process), \
                patch.object(runtime.windows, 'account_sid', return_value='other'):
            with self.assertRaisesRegex(ValueError, 'mismatch'):
                runtime.inspect_bound(host, 'term_1234abcd', binding)

    def test_session_inventory_requires_exactly_one_login(self):
        from shared.windows_session import selected_session
        for found in ([], [{'session_id': 3}, {'session_id': 4}]):
            with patch.object(runtime.windows, 'sessions', return_value=found), self.assertRaises(ValueError):
                selected_session('synthetic')

    def test_pythonw_wrapper_is_one_worker_when_inspected_by_python_cli(self):
        from api.mt5_ownership import inspect_processes
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            script = str(root / 'live/run_mt5_worker.py')
            args = ['-E', '-s', '-B', script, '--terminal-id', 'term_1234abcd', '--mode', 'live']
            launcher_dir = root / '.venv/Scripts'
            base_dir = root / 'base'
            parent = SimpleNamespace(pid=1, name=lambda: 'pythonw.exe',
                cmdline=lambda: [str(launcher_dir / 'pythonw.exe'), *args],
                create_time=lambda: 10, ppid=lambda: 999, exe=lambda: str(launcher_dir / 'pythonw.exe'))
            child = SimpleNamespace(pid=2, name=lambda: 'pythonw.exe',
                cmdline=lambda: [str(launcher_dir / 'pythonw.exe'), *args],
                create_time=lambda: 11, ppid=lambda: 1, exe=lambda: str(base_dir / 'pythonw.exe'))
            with patch('psutil.process_iter', return_value=[parent, child]), \
                    patch('api.mt5_ownership.sys.executable', str(launcher_dir / 'python.exe')), \
                    patch('api.mt5_ownership.sys._base_executable', str(base_dir / 'python.exe')):
                entries = inspect_processes(root, 'term_1234abcd')
            self.assertEqual([entry.pid for entry in entries], [2])

    def test_native_connector_is_never_called_after_activation_denial(self):
        from live import run_mt5_worker as worker
        with tempfile.TemporaryDirectory() as directory, patch.object(worker, 'ROOT', Path(directory)), \
                patch.object(runtime, 'assert_local_launch', side_effect=ValueError('wrong session')), \
                patch.object(worker._connector, 'initialize') as connect:
            with self.assertRaisesRegex(ValueError, 'wrong session'):
                worker._initialize_runtime_terminal('synthetic.exe', 'term_1234abcd', 'cancelled')
            connect.assert_not_called()


if __name__ == '__main__':
    unittest.main()
