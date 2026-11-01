"""Offline two-listener host signal/port/teardown tests; never bind a socket."""

import asyncio
from pathlib import Path
import signal
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch

from api.read_only import ReadOnlyPolicy
from api.read_only_live import LiveReadServices
from run_read_only import _host_signals, serve_live


class PublicHostShutdownTests(unittest.IsolatedAsyncioTestCase):
    """Drive explicit events and signal callbacks, never real OS control signals."""

    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.policy = ReadOnlyPolicy(symbols=('EURUSD',), sources={'public-market': 'fixture-source'},
            origins=('http://127.0.0.1:5181',), result_root=Path(self.temp.name))

    async def asyncTearDown(self):
        self.temp.cleanup()

    async def test_host_owns_repeated_signals_until_context_exit_then_restores_handlers(self):
        stopping = asyncio.Event()
        previous = signal.getsignal(signal.SIGTERM)
        with _host_signals(stopping):
            handler = signal.getsignal(signal.SIGTERM)
            self.assertTrue(callable(handler))
            handler(signal.SIGTERM, None)
            handler(signal.SIGTERM, None)
            self.assertTrue(stopping.is_set())
            self.assertIs(signal.getsignal(signal.SIGTERM), handler)
        self.assertIs(signal.getsignal(signal.SIGTERM), previous)

    async def test_invalid_or_colliding_listener_ports_fail_without_loading_runtime(self):
        for port, callback in ((8000, 8000), (True, 8000), (8811, 80), (65536, 8000)):
            with self.assertRaises(ValueError):
                await serve_live(self.policy, port, callback_port=callback)

    async def test_listener_return_without_requested_shutdown_is_a_failure_for_pm2(self):
        """A normal Uvicorn return after lifespan failure must not become exit zero."""
        servers = []
        class Server:
            def __init__(inner, config):
                inner.config = config
                inner.index = len(servers)
                inner.exit = asyncio.Event()
                servers.append(inner)
            @property
            def should_exit(inner):
                return inner.exit.is_set()
            @should_exit.setter
            def should_exit(inner, value):
                if value:
                    inner.exit.set()
            async def serve(inner):
                if inner.index == 0:
                    return  # Uvicorn can swallow lifespan startup failure this way.
                await inner.exit.wait()
        uvicorn = SimpleNamespace(Server=Server, Config=lambda app, **kwargs: SimpleNamespace(app=app, **kwargs))
        with patch.dict(sys.modules, {'uvicorn': uvicorn}), \
                patch.object(LiveReadServices, 'close', new=AsyncMock()) as close:
            with self.assertRaisesRegex(RuntimeError, 'without a requested shutdown'):
                await asyncio.wait_for(serve_live(self.policy, 8872, callback_port=8871), 2)
            close.assert_awaited_once()
        self.assertTrue(all(server.should_exit for server in servers))

    async def test_stop_event_and_repeated_cancellation_keep_callback_until_service_drain(self):
        events, servers = [], []
        started, draining, finish = asyncio.Event(), asyncio.Event(), asyncio.Event()
        stopping = asyncio.Event()
        async def close_service(_):
            events.append('service-drain')
            draining.set()
            await finish.wait()
            events.append('service-closed')
        class Server:
            """Record callback/visitor ownership without creating any actual server."""
            def __init__(inner, config):
                inner.config = config
                inner.index = len(servers)
                inner.exit = asyncio.Event()
                servers.append(inner)
            @property
            def should_exit(inner):
                return inner.exit.is_set()
            @should_exit.setter
            def should_exit(inner, value):
                if value:
                    events.append(f'exit-{inner.index}')
                    inner.exit.set()
            async def serve(inner):
                with inner.capture_signals():
                    if inner.index == 1:
                        started.set()
                    await inner.exit.wait()
        fake = SimpleNamespace(Server=Server, Config=lambda app, **kwargs: SimpleNamespace(app=app, **kwargs))
        previous = signal.getsignal(signal.SIGTERM)
        with patch.dict(sys.modules, {'uvicorn': fake}), \
                patch.object(LiveReadServices, 'close', new=close_service):
            task = asyncio.create_task(serve_live(self.policy, 8872, callback_port=8871, stop_event=stopping))
            try:
                await asyncio.wait_for(started.wait(), 2)
                self.assertEqual([s.config.host for s in servers], ['127.0.0.1', '127.0.0.1'])
                self.assertEqual([s.config.port for s in servers], [8871, 8872])
                stopping.set()
                await asyncio.wait_for(draining.wait(), 2)
                task.cancel()
                await asyncio.sleep(0)
                task.cancel()
                await asyncio.sleep(0)
                self.assertFalse(servers[0].should_exit)
                self.assertTrue(servers[1].should_exit)
                self.assertFalse(task.done())
            finally:
                finish.set()
                with self.assertRaises(asyncio.CancelledError):
                    await task
        self.assertLess(events.index('service-closed'), events.index('exit-0'))
        self.assertIs(signal.getsignal(signal.SIGTERM), previous)


if __name__ == '__main__':
    unittest.main()
