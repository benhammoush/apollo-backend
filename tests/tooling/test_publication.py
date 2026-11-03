"""Use synthetic disposable trees to exercise disclosure guards without private data."""

from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from scripts.check_publication import (
    REQUIRED,
    check_ignored,
    check_paths,
    check_text,
    public_path,
    source_paths,
    tracked_paths,
)


class PublicationTests(unittest.TestCase):
    """Make ignore bypasses, unsafe text and linked-path regressions fail closed."""

    def test_private_and_unknown_paths_are_rejected(self) -> None:
        """Private artifacts cannot enter through case aliases, traversal or allowed roots."""
        for name in (
            ".env",
            ".env.example",
            "visitor-policy.local.json",
            "docs/SETUP.md",
            "docs/setup.md",
            "api/config.local.json",
            "tests/real.csv",
            "cache/logs/a.log",
            ".venv/a.py",
            "live/native.exe",
            "shared/research.parquet",
            "shared/period_levels.py",
            "api/link.key",
            "api/../private.py",
            "/api/a.py",
            "api//a.py",
            "api\\a.py",
            "C:/a.py",
            ".github/workflows/deploy.yml",
            ".coderabbit.yaml",
            ".coderabbit.yml",
            "tests/fixtures/unreviewed.json",
        ):
            with self.subTest(name=name):
                self.assertFalse(public_path(name))

    def test_public_source_and_synthetic_fixture_are_admitted(self) -> None:
        """Required authored code, workflow and fixture kinds remain publishable."""
        for name in (
            "api/read_only.py",
            "tests/fixtures/result_contracts_v1.json",
            "scripts/check_publication.py",
            "docs/verification.md",
            "LICENSE.md",
            ".github/workflows/verify-pr.yml",
        ):
            with self.subTest(name=name):
                self.assertTrue(public_path(name))

    def test_complete_publication_does_not_require_retired_review_configuration(self) -> None:
        """The complete admitted source set must pass without the removed bot configuration."""
        self.assertNotIn(".coderabbit.yaml", REQUIRED)
        self.assertNotIn(".coderabbit.yml", REQUIRED)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for name in REQUIRED:
                path = root / name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text("Public fixture\n", encoding="utf-8")
            self.assertEqual(check_paths(root, source_paths(root)), len(REQUIRED))

    def test_credential_diagnostics_never_repeat_matches(self) -> None:
        """Construct fake secrets at runtime so the scanner has no fixture exemptions."""
        values = (
            "gh" + "p_" + "a" * 36,
            "github_" + "pat_" + "b" * 50,
            "AK" + "IA" + "A" * 16,
            "AI" + "za" + "C" * 35,
            "-----BEGIN " + "PRIVATE KEY-----",
            "https://" + "username:long-password@" + "example.invalid",
            'password = "' + "synthetic-long-value" + '"',
            'authtoken: "' + "synthetic-long-value" + '"',
            "binary\x00text",
        )
        for value in values:
            with self.subTest(length=len(value)):
                with self.assertRaises(ValueError) as error:
                    check_text(value, "docs/example.md")
                self.assertNotIn(value, str(error.exception))

    def test_missing_required_inputs_and_case_duplicates_fail(self) -> None:
        """An empty or case-colliding source set cannot pass as complete publication."""
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "README.md").write_text("Synthetic source", encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "missing"):
                check_paths(root, ["README.md"])
            with self.assertRaises(ValueError):
                check_paths(root, ["README.md", "README.md"], require_complete=False)
            self.assertEqual(check_paths(root, ["README.md"], require_complete=False), 1)

    def test_source_walk_ignores_installation_but_not_unknown_nested_files(self) -> None:
        """Preparation stays out of venvs; accidental artifacts in source still fail."""
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / ".venv").mkdir()
            (root / ".venv/private.py").write_text("not selected", encoding="utf-8")
            (root / "api").mkdir()
            (root / "api/private.csv").write_text("not source", encoding="utf-8")
            names = source_paths(root)
            self.assertEqual(names, ["api/private.csv"])
            with self.assertRaises(ValueError):
                check_paths(root, names, require_complete=False)

    def test_linked_parent_is_rejected_before_content_read(self) -> None:
        """Reject Windows junctions too, without needing symlink privileges in tests."""
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "api").mkdir()
            (root / "api/a.py").write_text("pass", encoding="utf-8")
            with patch.object(Path, "is_junction", return_value=True):
                with self.assertRaisesRegex(ValueError, "Linked"):
                    check_paths(root, ["api/a.py"], require_complete=False)

    def test_tracked_scan_includes_force_added_private_files(self) -> None:
        """Inspect the index directly rather than relying on Git ignore filtering."""
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / ".git").mkdir()
            record = f"100644 {'a' * 40} 0\t.env\0".encode()
            result = subprocess.CompletedProcess([], 0, record, b"")
            with patch("scripts.check_publication.subprocess.run", return_value=result) as run:
                with self.assertRaises(ValueError):
                    tracked_paths(root)
                self.assertNotIn("--exclude-standard", run.call_args.args[0])
                self.assertIn("--stage", run.call_args.args[0])

    def test_tracked_mode_does_not_fall_back_to_source_walk(self) -> None:
        """CI must not silently downgrade an absent repository into a partial scan."""
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaisesRegex(ValueError, "Git repository"):
                tracked_paths(Path(directory))

    def test_staged_secret_cannot_hide_behind_a_clean_working_copy(self) -> None:
        """The bytes about to be committed, not merely their filenames, must be scanned."""
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / ".git").mkdir()
            (root / "README.md").write_text("Clean working copy", encoding="utf-8")
            secret = ("gh" + "p_" + "a" * 36).encode()
            record = f"100644 {'a' * 40} 0\tREADME.md\0".encode()
            results = [
                subprocess.CompletedProcess([], 0, record, b""),
                subprocess.CompletedProcess([], 0, str(len(secret)).encode(), b""),
                subprocess.CompletedProcess([], 0, secret, b""),
            ]
            with patch("scripts.check_publication.subprocess.run", side_effect=results):
                with self.assertRaises(ValueError) as error:
                    tracked_paths(root)
                self.assertNotIn(secret.decode(), str(error.exception))

    def test_staged_symlink_mode_cannot_hide_behind_a_regular_working_file(self) -> None:
        """Reject indexed symlinks before reading their target or any blob contents."""
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / ".git").mkdir()
            (root / "README.md").write_text("Clean working copy", encoding="utf-8")
            record = f"120000 {'a' * 40} 0\tREADME.md\0".encode()
            result = subprocess.CompletedProcess([], 0, record, b"")
            with patch("scripts.check_publication.subprocess.run", return_value=result) as run:
                with self.assertRaises(ValueError):
                    tracked_paths(root)
                self.assertEqual(run.call_count, 1)

    def test_index_must_match_tested_source_except_line_endings(self) -> None:
        """Benign and malicious staged drift both fail; normal Git CRLF conversion may pass."""
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / ".git").mkdir()
            (root / "README.md").write_bytes(b"Checked text\r\n")
            record = f"100644 {'a' * 40} 0\tREADME.md\0".encode()
            for blob, accepted in ((b"Different staged text\n", False), (b"Checked text\n", True)):
                results = [
                    subprocess.CompletedProcess([], 0, record, b""),
                    subprocess.CompletedProcess([], 0, str(len(blob)).encode(), b""),
                    subprocess.CompletedProcess([], 0, blob, b""),
                ]
                with patch("scripts.check_publication.subprocess.run", side_effect=results):
                    if accepted:
                        self.assertEqual(tracked_paths(root), ["README.md"])
                    else:
                        with self.assertRaisesRegex(ValueError, "differs"):
                            tracked_paths(root)

    def test_required_inputs_cannot_be_hidden_by_ignore_rules(self) -> None:
        """Treat ignored inputs and Git inspection failures as publication failures."""
        for result in (
            subprocess.CompletedProcess([], 0, b"tests/fixtures/result_contracts_v1.json\0", b""),
            subprocess.CompletedProcess([], 128, b"", b"private diagnostic"),
        ):
            with patch("scripts.check_publication.subprocess.run", return_value=result):
                with self.assertRaises(ValueError):
                    check_ignored(Path("synthetic"), ["README.md"])
