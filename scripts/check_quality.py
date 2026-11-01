"""Verify source disclosure and synthetic behavior without operating a live backend."""

from __future__ import annotations

import argparse
import ast
from collections.abc import Sequence
import os
from pathlib import Path
import subprocess
import sys

from scripts.check_publication import check_ignored, check_paths, source_paths, tracked_paths
from scripts.check_notices import check as check_notices

STRICT_INPUTS = (
    "scripts",
    "tests/tooling",
    "tests/test_period_levels_removed.py",
    "tests/websocket_session.py",
    "tests/test_websocket_session.py",
)
LOCKS = ("requirements-public-win-py313.lock", "requirements-dev-win-py313.lock")


def run(root: Path, arguments: Sequence[str]) -> None:
    """Use the selected interpreter and propagate failures; never retry or suppress checks."""
    subprocess.run(
        [sys.executable, "-B", "-m", *arguments],
        cwd=root,
        env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"},
        check=True,
    )


def compile_sources(root: Path, names: Sequence[str]) -> None:
    """Compile every admitted Python file in memory without imports or bytecode writes."""
    for name in names:
        if name.endswith(".py"):
            compile((root / name).read_text(encoding="utf-8"), name, "exec", dont_inherit=True)


def check_docstrings(root: Path, names: Sequence[str]) -> None:
    """Require purpose docs for new modules and all named helpers, including private ones."""
    for name in names:
        if not name.endswith(".py") or not any(
            name == prefix or name.startswith(prefix + "/") for prefix in STRICT_INPUTS
        ):
            continue
        tree = ast.parse((root / name).read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
                if not ast.get_docstring(node):
                    raise ValueError(
                        f"Missing purpose documentation: {name}:{getattr(node, 'lineno', 1)}"
                    )


def audit(root: Path) -> None:
    """Audit runtime and full verification locks even if either fails; no ignores or fixes."""
    failed = False
    for lock in LOCKS:
        try:
            run(root, ("pip_audit", "--strict", "--disable-pip", "--require-hashes", "-r", lock))
        except subprocess.CalledProcessError:
            failed = True
    if failed:
        raise ValueError("Dependency advisory or audit-service failure; inspect audit output")


def main() -> None:
    """Require tracked verification by default; source mode is pre-Git preparation only."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", action="store_true", help="Explicit pre-Git preparation")
    parser.add_argument("--audit-only", action="store_true", help="Read-only scheduled audit")
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    names = source_paths(root) if args.source else tracked_paths(root)
    count = check_paths(root, names)
    check_notices(root / "THIRD_PARTY_NOTICES.txt")
    if not args.source:
        check_ignored(root, names)
    print(f"Verified publication boundary for {count} inputs.", flush=True)
    if not args.audit_only:
        compile_sources(root, names)
        check_docstrings(root, names)
        run(root, ("ruff", "check", "--no-cache", *STRICT_INPUTS))
        run(root, ("ruff", "format", "--check", "--no-cache", *STRICT_INPUTS))
        run(root, ("mypy", "--cache-dir", os.devnull))
        run(root, ("pip", "check"))
        run(root, ("unittest", "discover", "-s", "tests", "-v"))
    audit(root)


if __name__ == "__main__":
    main()
