"""Keep source-only CI credential-free and exercise fail-closed policy mutations."""

from __future__ import annotations

from copy import deepcopy
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import tomllib
from typing import Any, cast
import unittest
from unittest.mock import patch

import yaml

from scripts import check_quality
from scripts.check_notices import canonical, check as check_notices

ROOT = Path(__file__).resolve().parents[2]
INSTALL = (
    "python -m pip install --require-hashes --only-binary=:all: -r requirements-dev-win-py313.lock"
)
QUALITY = "python -B -m scripts.check_quality"
GATE = "if ($env:VERIFY_RESULT -ne 'success') { exit 1 }"
CHECKOUT = "actions/checkout@d23441a48e516b6c34aea4fa41551a30e30af803"
PYTHON = "actions/setup-python@ece7cb06caefa5fff74198d8649806c4678c61a1"


def workflow(name: str) -> dict[str, Any]:
    """Read strings without YAML 1.1's accidental conversion of the `on` key to True."""
    path = ROOT / ".github/workflows" / name
    return cast(dict[str, Any], yaml.load(path.read_text(encoding="utf-8"), Loader=yaml.BaseLoader))


def validate(data: dict[str, Any], name: str) -> None:
    """Restrict workflow capability and graph, not merely deployment-looking labels."""
    assert set(data) == {"name", "on", "permissions", "jobs"}
    assert data["permissions"] == {"contents": "read"}
    triggers = {
        "verify-pr.yml": {"pull_request"},
        "verify-main.yml": {"push", "workflow_dispatch"},
        "weekly-vuln-audit.yml": {"schedule", "workflow_dispatch"},
    }
    assert set(data["on"]) == triggers[name]
    if name == "verify-main.yml":
        assert data["on"]["push"] == {"branches": ["main"]}
    if name == "weekly-vuln-audit.yml":
        assert data["on"]["schedule"] == [{"cron": "41 7 * * 1"}]
    expected_jobs = (
        {"verify", "verify_result"}
        if name == "verify-pr.yml"
        else {"audit" if name == "weekly-vuln-audit.yml" else "verify"}
    )
    assert set(data["jobs"]) == expected_jobs
    for job_id, job in data["jobs"].items():
        assert job["runs-on"] == "windows-2025"
        assert 0 < int(job["timeout-minutes"]) <= 25
        allowed = {"name", "runs-on", "timeout-minutes", "steps"}
        if job_id == "verify_result":
            assert name == "verify-pr.yml"
            allowed |= {"needs", "if"}
            assert job["needs"] == "verify"
            assert job["if"] == "always()"
            assert job["steps"] == [
                {
                    "name": "Require successful PR checks",
                    "shell": "pwsh",
                    "env": {"VERIFY_RESULT": "${{ needs.verify.result }}"},
                    "run": GATE,
                }
            ]
        else:
            if name != "verify-pr.yml":
                allowed.add("if")
                assert job["if"] == "github.ref == 'refs/heads/main'"
            steps = job["steps"]
            assert len(steps) == 4
            assert steps[0] == {"uses": CHECKOUT, "with": {"persist-credentials": "false"}}
            assert steps[1] == {
                "uses": PYTHON,
                "with": {
                    "python-version": "3.13.16",
                    "architecture": "x64",
                },
            }
            assert set(steps[2]) == {"name", "run"} and steps[2]["run"] == INSTALL
            command = QUALITY + (" --audit-only" if job_id == "audit" else "")
            assert set(steps[3]) == {"name", "run"} and steps[3]["run"] == command
        assert set(job) == allowed


class CiTests(unittest.TestCase):
    """Guard the native Python CI exception without introducing a deployment pipeline."""

    def test_all_workflows_follow_source_only_policy(self) -> None:
        """PR, exact-main and weekly checks must retain their declared minimal capabilities."""
        for name in ("verify-pr.yml", "verify-main.yml", "weekly-vuln-audit.yml"):
            with self.subTest(name=name):
                validate(workflow(name), name)

    def test_notice_regeneration_preserves_licence_wording(self) -> None:
        """Only line endings and trailing whitespace may change in generated notices."""
        text = "Synthetic licence.  \r\r\nRetain copyright.\t\r\n\r\n"
        self.assertEqual(canonical(text), "Synthetic licence.\nRetain copyright.\n")
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "notices.txt"
            path.write_bytes(text.encode())
            with self.assertRaises(ValueError):
                check_notices(path)
            check_notices(path, write=True)
            self.assertEqual("".join(text.split()), "".join(path.read_text().split()))
            check_notices(path)

    def test_unsafe_workflow_mutations_fail(self) -> None:
        """Credential access, self-hosting, mutable actions and successful no-ops are rejected."""
        original = workflow("verify-main.yml")
        mutations: list[dict[str, Any]] = []
        for target, key, value in (
            ("top", "permissions", {"contents": "write"}),
            ("top", "on", {"pull_request_target": ""}),
            ("job", "runs-on", "self-hosted"),
            ("job", "environment", "production"),
            ("job", "secrets", "inherit"),
            ("job", "if", "always()"),
        ):
            data = deepcopy(original)
            (data if target == "top" else data["jobs"]["verify"])[key] = value
            mutations.append(data)
        for step, key, value in (
            (0, "uses", "actions/checkout@main"),
            (0, "with", {"ref": "some-other-source", "persist-credentials": "false"}),
            (2, "run", "echo success"),
            (3, "run", QUALITY + " --source"),
        ):
            data = deepcopy(original)
            data["jobs"]["verify"]["steps"][step][key] = value
            mutations.append(data)
        for data in mutations:
            with self.assertRaises(AssertionError):
                validate(data, "verify-main.yml")

    def test_pr_gate_cannot_hide_failed_or_skipped_verification(self) -> None:
        """The always-running aggregate gate must depend on verification and test success."""
        data = workflow("verify-pr.yml")
        data["jobs"]["verify_result"]["steps"][0]["run"] = "exit 0"
        with self.assertRaises(AssertionError):
            validate(data, "verify-pr.yml")

    def test_both_audits_run_and_any_failure_propagates(self) -> None:
        """Audit both trees even on failure; no skip or advisory allowlist is supported."""
        with patch.object(
            check_quality, "run", side_effect=[subprocess.CalledProcessError(1, []), None]
        ) as run:
            with self.assertRaises(ValueError):
                check_quality.audit(ROOT)
            self.assertEqual(run.call_count, 2)
            for call in run.call_args_list:
                arguments = call.args[1]
                self.assertIn("--strict", arguments)
                self.assertIn("--require-hashes", arguments)
                self.assertNotIn("--ignore-vuln", arguments)

    def test_quality_scope_matches_documented_legacy_exception(self) -> None:
        """The new tooling stays strict; legacy source keeps syntax and synthetic coverage."""
        config = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
        self.assertTrue(config["tool"]["mypy"]["strict"])
        self.assertEqual(config["tool"]["mypy"]["files"], list(check_quality.STRICT_INPUTS))
        self.assertEqual(
            config["tool"]["mypy"]["overrides"],
            [{"module": ["api.*", "live.*", "shared.*"], "follow_imports": "skip"}],
        )
        self.assertNotIn("ignore", config["tool"]["ruff"]["lint"])
        docs = (ROOT / "docs/verification.md").read_text(encoding="utf-8")
        self.assertIn("Legacy quality exception", docs)

    def test_missing_private_helper_docs_and_invalid_types_fail(self) -> None:
        """Actual tools reject undocumented internal helpers and deliberately wrong types."""
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "scripts").mkdir()
            sample = root / "scripts/sample.py"
            sample.write_text('"""Fixture."""\ndef _helper():\n    return 1\n', encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "purpose documentation"):
                check_quality.check_docstrings(root, ["scripts/sample.py"])
            sample.write_text('"""Fixture."""\nvalue: int = "not an integer"\n', encoding="utf-8")
            result = subprocess.run(
                [sys.executable, "-B", "-m", "mypy", "--strict", "--cache-dir", "nul", str(sample)],
                cwd=root,
                capture_output=True,
            )
            self.assertEqual(result.returncode, 1)
            self.assertIn(b"assignment", result.stdout)

    def test_new_code_lint_and_format_gates_reject_bad_samples(self) -> None:
        """Enforce documentation/lint and formatting behavior, not just config presence."""
        for command, code in (
            (
                ("check", "--select", "F,D100,D103", "--stdin-filename", "sample.py", "-"),
                b"def helper():\n    return undefined_name\n",
            ),
            (
                ("format", "--check", "--stdin-filename", "sample.py", "-"),
                b'"""Fixture."""\nx=  1\n',
            ),
        ):
            result = subprocess.run(
                [sys.executable, "-B", "-m", "ruff", *command],
                cwd=ROOT,
                input=code,
                capture_output=True,
            )
            self.assertEqual(result.returncode, 1)

    def test_runtime_and_tool_pins_are_not_ranges_or_private_sources(self) -> None:
        """Verification installation must retain the original runtime pins and public index."""
        for name in check_quality.LOCKS:
            text = (ROOT / name).read_text(encoding="utf-8")
            self.assertNotRegex(text, r"(?im)^\s*(?:--index-url|--extra-index-url|-e |git\+)")
            self.assertIn("--hash=sha256:", text)
        tool_input = (ROOT / "requirements-dev.txt").read_text(encoding="utf-8")
        self.assertIn("-r requirements-public-win-py313.lock", tool_input)
        runtime = dict(
            re.findall(
                r"(?m)^([a-zA-Z0-9_.-]+)==([^\s;]+)", (ROOT / check_quality.LOCKS[0]).read_text()
            )
        )
        full = dict(
            re.findall(
                r"(?m)^([a-zA-Z0-9_.-]+)==([^\s;]+)", (ROOT / check_quality.LOCKS[1]).read_text()
            )
        )
        for package, version in runtime.items():
            self.assertEqual(full[package], version)
