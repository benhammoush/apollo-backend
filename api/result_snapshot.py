"""Explicit offline table preparation and read-only indexed paging, never CSV discovery.

Only trusted operator code calls build_snapshot after content/rights review.
Visitor requests never prepare snapshots, scan source CSVs or create indexes.
"""

import csv
from contextlib import closing
import hashlib
import json
from math import isfinite
import os
from pathlib import Path
import sqlite3
import uuid

from api.result_contracts import TABLE_SCHEMA_VERSION, validate_table_columns

MAX_SNAPSHOT_BYTES = 2_000_000_000
MAX_SOURCE_ROWS = 2_000_000
MAX_RECORD_BYTES = 1_000_000


class _CsvLines:
    """Bound physical and multiline logical records while hashing original bytes."""

    def __init__(self, stream, max_bytes=MAX_SNAPSHOT_BYTES):
        """Own only a caller-supplied source stream, never a path chosen by a visitor."""
        self.stream = stream
        self.digest = hashlib.sha256()
        self.remaining = MAX_RECORD_BYTES
        self.total = 0
        self.first = True
        self.max_bytes = max_bytes

    def __iter__(self):
        """Supply physical lines to the CSV parser without buffering the file."""
        return self

    def __next__(self):
        """Reject a record before decoding if its cumulative bytes exceed the budget."""
        remaining = min(self.remaining, self.max_bytes - self.total)
        raw = self.stream.readline(remaining + 1)
        if not raw:
            raise StopIteration
        if len(raw) > remaining:
            raise ValueError("CSV record exceeds preparation budget")
        self.remaining -= len(raw)
        self.total += len(raw)
        self.digest.update(raw)
        text = raw.decode("utf-8-sig" if self.first else "utf-8")
        self.first = False
        return text


def build_snapshot(source: Path, target: Path, columns: tuple[str, ...], *, date_columns: tuple[str, ...] | None = None,
                   max_source_bytes: int = MAX_SNAPSHOT_BYTES, max_snapshot_bytes: int = MAX_SNAPSHOT_BYTES) -> dict:
    """Prepare an exclusive immutable artifact; never overwrite an existing target.

    This validates structure, not secrets/rights. Callers must approve contents
    separately. No source paths or companion metadata enter the artifact.
    """
    from shared.csv_read import convert_value_type

    validate_table_columns(columns)
    if any(type(value) is not int or not 1 <= value <= MAX_SNAPSHOT_BYTES
           for value in (max_source_bytes, max_snapshot_bytes)):
        raise ValueError("Invalid preparation byte budget")
    if date_columns is not None:
        from api.result_masking import MASK_VERSION, masked_cells, validate_date_columns
        validate_date_columns(columns, date_columns)
    source, target = Path(source), Path(target)
    if source.suffix.lower() != ".csv" or target.suffix.lower() != ".sqlite" or target.exists():
        raise ValueError("Invalid snapshot preparation target")
    if source.stat().st_size > max_source_bytes:
        raise ValueError("Source exceeds preparation budget")
    temporary = target.with_name(f".{uuid.uuid4().hex}.sqlite")
    temporary.touch(exist_ok=False)
    connection = None
    try:
        connection = sqlite3.connect(temporary)
        connection.execute("CREATE TABLE result_rows (row_number INTEGER PRIMARY KEY, data TEXT NOT NULL)")
        connection.execute("CREATE TABLE metadata (name TEXT PRIMARY KEY, value TEXT NOT NULL)")
        count = 0
        prepared_bytes = 0
        masked_digest = hashlib.sha256(json.dumps({"columns": columns, "dates": date_columns}).encode())
        with source.open("rb") as stream:
            identity = os.fstat(stream.fileno())
            lines = _CsvLines(stream, max_bytes=max_source_bytes)
            reader = csv.reader(lines, strict=True)
            if tuple(next(reader)) != columns:
                raise ValueError("CSV header differs from approved catalog")
            while True:
                lines.remaining = MAX_RECORD_BYTES
                try:
                    cells = next(reader)
                except StopIteration:
                    break
                if len(cells) != len(columns) or count >= MAX_SOURCE_ROWS:
                    raise ValueError("Invalid or oversized source rows")
                values = (masked_cells(columns, cells, date_columns) if date_columns is not None
                          else [convert_value_type(value, column) for column, value in zip(columns, cells)])
                if any(type(value) not in (str, int, float, bool, type(None))
                       or type(value) is float and not isfinite(value) for value in values):
                    raise ValueError("Snapshot cells must be finite JSON scalars")
                encoded = json.dumps(values, ensure_ascii=True, allow_nan=False, separators=(",", ":"))
                if len(encoded) > MAX_RECORD_BYTES:
                    raise ValueError("Encoded row exceeds preparation budget")
                prepared_bytes += len(encoded) + 128
                if prepared_bytes > max_snapshot_bytes // 2:
                    raise ValueError("Prepared rows exceed snapshot budget")
                connection.execute("INSERT INTO result_rows VALUES (?, ?)", (count, encoded))
                masked_digest.update(encoded.encode("ascii") + b"\n")
                count += 1
            after = os.fstat(stream.fileno())
            if (identity.st_size, identity.st_mtime_ns) != (after.st_size, after.st_mtime_ns):
                raise ValueError("Source changed during preparation")
        metadata = {"version": TABLE_SCHEMA_VERSION, "columns": list(columns),
                    "row_count": count, "revision": masked_digest.hexdigest() if date_columns is not None else lines.digest.hexdigest()}
        if date_columns is not None:
            metadata["masking"] = {"version": MASK_VERSION, "date_columns": list(date_columns)}
        encoded_metadata = json.dumps(metadata)
        if len(encoded_metadata) > 600_000:
            raise ValueError("Snapshot descriptor exceeds read budget")
        connection.execute("INSERT INTO metadata VALUES ('descriptor', ?)", (encoded_metadata,))
        connection.commit()
        connection.close()
        connection = None
        if temporary.stat().st_size > max_snapshot_bytes:
            raise ValueError("Snapshot exceeds preparation budget")
        # Same-directory hard-link creation is exclusive: an existing artifact
        # cannot be replaced by a race between an existence check and rename.
        os.link(temporary, target)
        return metadata
    finally:
        if connection is not None:
            connection.close()
        temporary.unlink(missing_ok=True)


def page_size(description, policy) -> int:
    """Bound both rows and total cells for very wide all-column layouts."""
    return min(policy.max_rows, 200, max(1, 20_000 // len(description.columns)))


def file_identity(path: Path) -> tuple:
    """Detect replacement or in-place writes under trusted immutable-artifact ownership."""
    stat = path.stat()
    return stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns


def read_snapshot(selection, policy, offset: int, limit: int) -> tuple[list, int]:
    """Read a pinned bounded page using a primary-key range, never OFFSET or a CSV scan."""
    from fastapi import HTTPException

    description = selection.description
    path = Path(selection.path).resolve()
    if not path.is_relative_to(policy.result_root) or path.suffix.lower() != ".sqlite":
        raise HTTPException(404)
    if not 0 <= offset <= description.row_count or not 1 <= limit <= page_size(description, policy):
        raise HTTPException(422)
    if path.stat().st_size > MAX_SNAPSHOT_BYTES:
        raise HTTPException(413)
    if file_identity(path) != selection.snapshot_identity:
        raise HTTPException(409)
    with path.open("rb") as stream:
        header = stream.read(100)
    # Prepared standalone rollback-mode artifacts need no WAL/SHM side files.
    # Reject other modes before opening SQLite, rather than letting a read-only
    # connection initialize shared-memory files in an operator-owned directory.
    if (len(header) != 100 or header[:16] != b"SQLite format 3\x00"
            or header[18:20] != b"\x01\x01"
            or any(Path(str(path) + suffix).exists() for suffix in ("-wal", "-shm", "-journal"))):
        raise HTTPException(409)
    expected = {"version": TABLE_SCHEMA_VERSION, "columns": list(description.columns),
                "row_count": description.row_count, "revision": description.revision}
    if description.date_columns is not None:
        from api.result_masking import MASK_VERSION
        expected["masking"] = {"version": MASK_VERSION, "date_columns": list(description.date_columns)}
    with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True, timeout=1)) as connection:
        # The VM counter cannot bound work inside length()/substr(). Bound
        # SQLite's own row/string allocations before executing any statements.
        connection.setlimit(sqlite3.SQLITE_LIMIT_LENGTH, MAX_RECORD_BYTES + 100_000)
        connection.execute("PRAGMA query_only=ON")
        connection.execute("PRAGMA cache_size=-1024")
        # Fixed statements still get a VM budget, guarding malformed/unindexed
        # operator artifacts. HTTP admission remains charged until this returns.
        steps = 0

        def budget():
            """Interrupt unexpected full scans rather than consuming unbounded work."""
            nonlocal steps
            steps += 1000
            return steps > 100_000

        connection.set_progress_handler(budget, 1000)
        connection.execute("BEGIN")
        for table, expected_columns in (
                ("result_rows", [("row_number", "INTEGER", 1), ("data", "TEXT", 0)]),
                ("metadata", [("name", "TEXT", 1), ("value", "TEXT", 0)])):
            layout = connection.execute(f"PRAGMA table_info('{table}')").fetchmany(3)
            if [(item[1], item[2], item[5]) for item in layout] != expected_columns:
                raise HTTPException(409)
        record = connection.execute("SELECT substr(value,1,600001) FROM metadata WHERE name='descriptor'").fetchone()
        if record is None or len(record[0]) > 600000 or json.loads(record[0]) != expected:
            raise HTTPException(409)
        cursor = connection.execute(
            "SELECT row_number,length(data),substr(data,1,?) FROM result_rows "
            "WHERE row_number>=? AND row_number<? ORDER BY row_number LIMIT ?",
            (MAX_RECORD_BYTES + 1, offset, min(description.row_count, offset + limit), limit))
        rows = []
        # Reserve the descriptor/column catalog and envelope, using object-row
        # encoding as a conservative bound for both JSON wire representations.
        size = 2 * len(json.dumps(description.to_dict(), ensure_ascii=False).encode("utf-8")) + 4096
        byte_limited = False
        for number, length, encoded in cursor:
            if number != offset + len(rows) or length > MAX_RECORD_BYTES:
                raise HTTPException(409)
            cells = json.loads(encoded)
            if not isinstance(cells, list) or len(cells) != len(description.columns):
                raise HTTPException(409)
            for value in cells:
                if type(value) not in (str, int, float, bool, type(None)):
                    raise HTTPException(409)
                if type(value) is float and not isfinite(value):
                    raise HTTPException(409)
            if description.date_columns is not None:
                from api.result_masking import validate_masked_cells
                try:
                    validate_masked_cells(description.columns, cells, description.date_columns)
                except ValueError:
                    raise HTTPException(409) from None
            row = dict(zip(description.columns, cells))
            if description.date_columns is None and any(name in row and row[name] not in description.symbols for name in ("pair", "symbol")):
                raise HTTPException(422)
            row_bytes = len(json.dumps(row, ensure_ascii=False, allow_nan=False).encode("utf-8")) + 1
            if size + row_bytes > policy.max_bytes:
                if not rows:
                    raise HTTPException(413)
                byte_limited = True
                break
            size += row_bytes
            rows.append(row)
        if not byte_limited and len(rows) != min(limit, description.row_count - offset):
            raise HTTPException(409)
        if file_identity(path) != selection.snapshot_identity:
            raise HTTPException(409)
        return rows, description.row_count
