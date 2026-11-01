"""Keep generated runtime notices readable without changing any licence wording."""

from __future__ import annotations

import argparse
from pathlib import Path


def canonical(text: str) -> str:
    """Normalize repeated Windows line endings and trailing whitespace, preserving words."""
    text = text.replace("\r\r\n", "\n")
    return "\n".join(line.rstrip() for line in text.splitlines()).rstrip() + "\n"


def check(path: Path, *, write: bool = False) -> None:
    """Check generated notice formatting or explicitly rewrite whitespace only."""
    original = path.read_bytes().decode("utf-8")
    normalized = canonical(original)
    if "".join(original.split()) != "".join(normalized.split()):
        raise ValueError("Notice wording changed during whitespace normalization")
    if write:
        path.write_bytes(normalized.encode("utf-8"))
    elif original != normalized:
        raise ValueError("Notice whitespace is not canonical; run scripts.check_notices --write")


def main() -> None:
    """Check the generated asset by default; regeneration requires an explicit flag."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--write", action="store_true", help="Normalize whitespace only")
    args = parser.parse_args()
    check(Path(__file__).resolve().parents[1] / "THIRD_PARTY_NOTICES.txt", write=args.write)


if __name__ == "__main__":
    main()
