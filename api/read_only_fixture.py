"""Synthetic local acceptance fixture; never imports the operational application."""

from contextlib import contextmanager
import csv
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import tempfile

from api.read_only import ReadOnlyApplication, ReadOnlyPolicy, SelectedResult
from api.result_contracts import DatasetDescriptor, SCHEMA_VERSION


class FixtureServices:
    """Deterministic fake market transport, clearly labeled as non-live data."""

    async def available_sources(self):
        """Advertise only the fixture source, never local terminals."""
        return {"synthetic-source"}

    async def market(self, symbol, tf, source, start, end):
        """Generate synthetic candle shapes within the requested date window."""
        minutes = {"M1": 1, "M5": 5, "M15": 15, "H1": 60, "H4": 240,
                   "D1": 1440, "W1": 10080, "MN1": 43200}[tf]
        first = datetime.fromisoformat(start).replace(tzinfo=timezone.utc)
        last = datetime.fromisoformat(end).replace(tzinfo=timezone.utc) + timedelta(days=1)
        count = min(int((last - first).total_seconds() // (minutes * 60)), 20000)
        return [{"time": int((first + timedelta(minutes=minutes * i)).timestamp()),
                 "open": 1.1 + (i % 20) / 10000, "high": 1.103, "low": 1.099,
                 "close": 1.1001 + (i % 20) / 10000, "volume": 10} for i in range(count)]

async def fixture_websocket(scope, receive, send):
    """Exercise the real public filter with synthetic tick and private-event fixtures."""
    await receive()
    await send({"type": "websocket.accept"})
    while True:
        message = await receive()
        if message["type"] == "websocket.disconnect":
            return
        if message.get("text") == "ping":
            await send({"type": "websocket.send", "text": "pong"})
        else:
            data = json.loads(message["text"])
            if data["type"] == "subscribe":
                for symbol in data["symbols"]:
                    event = {"type": "tick", "data": {"symbol": symbol, "source": "synthetic-source",
                             "bid": 1.1, "ask": 1.1001, "time_ms": 1704067200000}}
                    await send({"type": "websocket.send", "text": json.dumps(event)})
                await send({"type": "websocket.send", "text": json.dumps({"type": "job_error", "fixture": True})})


@contextmanager
def fixture_application(origins, *, full_table=False, masked_showcase=False):
    """Create disposable synthetic selections, with no real cache/env/PID reads."""
    with tempfile.TemporaryDirectory(prefix="apollo-public-fixture-") as directory:
        root = Path(directory)
        fixture = Path(__file__).resolve().parents[1] / "tests" / "fixtures" / "result_contracts_v1.json"
        rows = json.loads(fixture.read_text(encoding="utf-8"))
        selections = []
        for kind, raw_row in rows.items():
            row = {**raw_row}
            if "pair" in row:
                row["pair"] = "EURUSD"
            path = root / f"{kind}.csv"
            with path.open("w", newline="", encoding="utf-8") as stream:
                writer = csv.DictWriter(stream, fieldnames=list(row))
                writer.writeheader()
                writer.writerow({key: (str(value).lower() if type(value) is bool else value)
                                 for key, value in row.items()})
            description = DatasetDescriptor(
                dataset_id=f"demo-{kind.replace('_', '-')}", result_type=kind, schema_version=SCHEMA_VERSION,
                source_id="demo", symbols=("EURUSD",), timeframes=({"bb": ("M15",), "trends": ("H1",),
                    "families": ("MN1",), "levels": ("D1",), "session_levels": ("D1",)}.get(kind, ("M5",))),
                coverage_start="2024-01-01T00:00:00Z", coverage_end="2024-01-02T00:00:00Z",
                generated_at="2024-01-03T00:00:00Z",
            )
            selections.append(SelectedResult(description, path))
        if full_table:
            from api.result_contracts import TABLE_SCHEMA_VERSION
            from api.result_snapshot import build_snapshot
            columns = ("time", "pair", "extra_metric", "label", "flag")
            source = root / "full-table.csv"
            with source.open("w", newline="", encoding="utf-8") as stream:
                writer = csv.writer(stream)
                writer.writerow(columns)
                for number in range(405):
                    writer.writerow((1704067200 + number, "EURUSD", number, "synthetic", "false"))
            artifact = root / "full-table.sqlite"
            built = build_snapshot(source, artifact, columns)
            selections.append(SelectedResult(DatasetDescriptor(dataset_id="demo-full-table", result_type="table",
                schema_version=TABLE_SCHEMA_VERSION, source_id="demo", symbols=("EURUSD",), timeframes=("H1",),
                coverage_start="2024-01-01T00:00:00Z", coverage_end="2024-01-02T00:00:00Z",
                generated_at="2024-01-03T00:00:00Z", columns=columns,
                revision=built["revision"], row_count=built["row_count"]), artifact))
        sources = {"demo": "synthetic-source"}
        if masked_showcase:
            from api.result_contracts import TABLE_SCHEMA_VERSION
            from api.result_snapshot import build_snapshot
            selections = []
            sources = {"masked-preview": "synthetic-source"}
            columns = ("datetime", "pair", "price", "flag", "note")
            for indicator in ("Triggers_BT", "trends"):
                source = root / f"private-{indicator}.csv"
                with source.open("w", newline="", encoding="utf-8") as stream:
                    writer = csv.writer(stream)
                    writer.writerow(columns)
                    for number in range(405):
                        writer.writerow((f"2024-01-01T00:{number % 60:02d}:00Z", "PRIVATE_PAIR", 123.45, "true", "PRIVATE_FIXTURE_ONLY"))
                artifact = root / f"masked-{indicator}.sqlite"
                built = build_snapshot(source, artifact, columns, date_columns=("datetime",))
                selections.append(SelectedResult(DatasetDescriptor(dataset_id=f"masked-{indicator.lower()}",
                    result_type="table", schema_version=TABLE_SCHEMA_VERSION, source_id="masked-preview",
                    symbols=(), timeframes=(), coverage_start=None, coverage_end=None,
                    generated_at="2026-10-17T00:00:00Z", columns=columns, row_count=built["row_count"],
                    revision=built["revision"], date_columns=("datetime",), indicator=indicator,
                    filename="EURUSD.csv"), artifact))
        policy = ReadOnlyPolicy(symbols=("EURUSD",), sources=sources,
                                 origins=tuple(origins), result_root=root, results=tuple(selections),
                                 max_rows=25000, max_bytes=8_000_000, fixture=True,
                                 masked_results_only=masked_showcase)
        yield ReadOnlyApplication(policy, FixtureServices(), fixture_websocket)
