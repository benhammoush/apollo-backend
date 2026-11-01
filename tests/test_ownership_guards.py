"""Exercise review fixes with synthetic callbacks, targets and disposable lock files."""

import os
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, Mock, patch

import httpx

from api.live_runtime import create_live_app
from api.mt5_binding import target_revision
from api.mt5_ownership import LocalMT5PairOwnership
from shared.mt5_ownership import FileFence, OwnershipBusy


class RegistrationGuardTests(unittest.IsolatedAsyncioTestCase):
    """A selected source alone is not proof of a worker's process ownership."""

    async def test_missing_host_rejects_registration_without_registry_access(self) -> None:
        """The standalone callback app must deny even a plausible selected worker."""
        tid = "term_1234abcd"
        app = create_live_app(
            SimpleNamespace(sources={"market": "synthetic-source"}, symbols=("EURUSD",)),
            recovery_terminal_ids=(tid,),
        )
        with (
            patch("api.live_runtime.worker_registry.get_pid", new_callable=AsyncMock) as get_pid,
            patch("api.live_runtime.worker_registry.register", new_callable=AsyncMock) as register,
        ):
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app, client=("127.0.0.1", 1)),
                base_url="http://127.0.0.1",
            ) as client:
                response = await client.post(
                    "/internal/worker/register",
                    json={
                        "worker_id": f"mt5_history:{tid}",
                        "pid": 1234,
                        "port": 9999,
                        "mql5_source": "synthetic-source",
                        "worker_type": "mt5_history",
                        "terminal_id": tid,
                    },
                )
            self.assertEqual(response.status_code, 403)
            get_pid.assert_not_awaited()
            register.assert_not_awaited()

    async def test_host_binding_still_rejects_unselected_or_unowned_workers(self) -> None:
        """Having a host must not bypass its target and exact process checks."""
        tid = "term_1234abcd"
        app = create_live_app(
            SimpleNamespace(sources={"market": "synthetic-source"}, symbols=("EURUSD",)),
            recovery_terminal_ids=(tid,),
        )
        app.state.mt5_host = SimpleNamespace(
            config_path=Path("unused-synthetic.json"),
            root=Path("."),
            runtime_managed=False,
            inspect=lambda *_: [SimpleNamespace(pid=1234, mode="history")],
        )
        with (
            patch("api.mt5_binding.target_revision", return_value=("synthetic", True)),
            patch("api.live_runtime.worker_registry.register", new_callable=AsyncMock) as register,
        ):
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app, client=("127.0.0.1", 1)),
                base_url="http://127.0.0.1",
            ) as client:
                for terminal, pid in (("term_deadbeef", 1234), (tid, 5678)):
                    response = await client.post(
                        "/internal/worker/register",
                        json={
                            "worker_id": f"mt5_history:{terminal}",
                            "pid": pid,
                            "port": 9999,
                            "mql5_source": "synthetic-source",
                            "worker_type": "mt5_history",
                            "terminal_id": terminal,
                        },
                    )
                    self.assertEqual(response.status_code, 403)
            register.assert_not_awaited()


class AdapterGuardTests(unittest.IsolatedAsyncioTestCase):
    """The unwired adapter cannot pretend to be an activated synchronous launcher."""

    async def test_start_is_explicitly_unsupported_and_still_validates_target(self) -> None:
        """A valid pin cannot launch, and changed pins still fail before unsupported start."""
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config = root / "cache/mt5/config.json"
            config.parent.mkdir(parents=True)
            config.write_text(
                '{"terminals":[{"id":"term_1234abcd",'
                '"terminal_path":"synthetic-terminal.exe","active":true}]}',
                encoding="utf-8",
            )
            revision, _ = target_revision(config, "term_1234abcd", require_active=True)
            target = SimpleNamespace(
                terminal_id="term_1234abcd",
                config_revision=revision,
                expected_source="synthetic-source",
            )
            adapter = LocalMT5PairOwnership(root)
            with (
                patch("api.mt5_host.MT5Host.start_pair") as launch,
                patch("api.mt5_ownership.inspect_processes", return_value=[]),
            ):
                with self.assertRaisesRegex(NotImplementedError, "request_launch"):
                    await adapter.start_if_absent(target)
                target.config_revision = "changed"
                with self.assertRaisesRegex(ValueError, "target changed"):
                    await adapter.start_if_absent(target)
                launch.assert_not_called()


class FileFenceReleaseTests(unittest.TestCase):
    """Explicit Windows unlock must precede close without losing error cleanup."""

    @unittest.skipUnless(os.name == "nt", "Windows byte-range locking")
    def test_windows_unlocks_first_byte_before_close(self) -> None:
        """Release rewinds to the acquired byte and unlocks before closing the stream."""
        import msvcrt

        fence = FileFence(Path("unused-synthetic.lock"))
        stream = Mock()
        fence.stream = stream
        events = Mock()
        events.attach_mock(stream, "stream")
        with patch("msvcrt.locking") as locking:
            events.attach_mock(locking, "locking")
            fence.release()
            self.assertEqual(events.method_calls[0].args, (0,))
            locking.assert_called_once_with(stream.fileno.return_value, msvcrt.LK_UNLCK, 1)
            self.assertLess(
                [call[0] for call in events.mock_calls].index("locking"),
                [call[0] for call in events.mock_calls].index("stream.close"),
            )
        self.assertIsNone(fence.stream)
        stream.close.assert_called_once()
        fence.release()
        stream.close.assert_called_once()

    @unittest.skipUnless(os.name == "nt", "Windows byte-range locking")
    def test_unlock_failure_still_closes_and_clears_stream(self) -> None:
        """An unlock error propagates only after cleanup, leaving release idempotent."""
        fence = FileFence(Path("unused-synthetic.lock"))
        stream = Mock()
        fence.stream = stream
        with patch("msvcrt.locking", side_effect=OSError("synthetic unlock failure")):
            with self.assertRaisesRegex(OSError, "unlock failure"):
                fence.release()
        stream.close.assert_called_once()
        self.assertIsNone(fence.stream)
        fence.release()

    def test_real_fence_can_be_reacquired_after_release(self) -> None:
        """Disposable native locks retain exclusion and permit immediate handoff."""
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "synthetic.lock"
            first, second = FileFence(path), FileFence(path)
            with first:
                with self.assertRaises(OwnershipBusy):
                    second.acquire()
            with second:
                self.assertTrue(path.exists())

    def test_non_windows_release_only_closes_and_clears(self) -> None:
        """Other platforms keep their existing close-based release behavior."""
        fence = FileFence(Path("unused-synthetic.lock"))
        stream = Mock()
        fence.stream = stream
        with patch("shared.mt5_ownership.os.name", "posix"):
            fence.release()
        stream.seek.assert_not_called()
        stream.close.assert_called_once()
        self.assertIsNone(fence.stream)

    def test_close_failure_still_clears_stream(self) -> None:
        """A failed close is visible without retaining an unusable handle in the fence."""
        fence = FileFence(Path("unused-synthetic.lock"))
        stream = Mock()
        stream.close.side_effect = OSError("synthetic close failure")
        fence.stream = stream
        with patch("shared.mt5_ownership.os.name", "posix"):
            with self.assertRaisesRegex(OSError, "close failure"):
                fence.release()
        self.assertIsNone(fence.stream)
