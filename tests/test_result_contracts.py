"""Synthetic-only checks for display field projection and dataset descriptions."""

from dataclasses import FrozenInstanceError, replace
import json
from pathlib import Path
import unittest

from api.result_contracts import (
    DatasetDescriptor,
    RESULT_SCHEMAS,
    SCHEMA_VERSION,
    get_result_schema,
    project_result_row,
)


class ResultContractTests(unittest.TestCase):
    """Keep display values exact and reject unrecognized layouts/metadata shapes."""

    def setUp(self):
        """Load hand-written synthetic rows, never application cache fixtures."""
        fixture = Path(__file__).parent / "fixtures" / "result_contracts_v1.json"
        self.rows = json.loads(fixture.read_text(encoding="utf-8"))

    def descriptor(self, **changes):
        """Build an explicit synthetic descriptor with no physical storage path."""
        fields = {
            "dataset_id": "fixture-bb", "result_type": "bb", "schema_version": SCHEMA_VERSION,
            "source_id": "fixture-source", "symbols": ("TEST",), "timeframes": ("M5",),
            "coverage_start": "2024-01-01T00:00:00Z", "coverage_end": "2024-01-02T00:00:00Z",
            "generated_at": "2024-01-03T00:00:00+00:00",
        }
        return DatasetDescriptor(**(fields | changes))

    def test_all_synthetic_rows_keep_values_types_and_order(self):
        """Every registered result family has a lossless scalar fixture."""
        self.assertEqual(set(self.rows), set(RESULT_SCHEMAS))
        for kind, row in self.rows.items():
            with self.subTest(kind=kind):
                projected = project_result_row(kind, SCHEMA_VERSION, row)
                self.assertEqual(projected, row)
                self.assertEqual(list(projected), list(RESULT_SCHEMAS[kind].fields))
                for key in row:
                    self.assertIs(type(projected[key]), type(row[key]))

    def test_unlisted_fields_and_nested_metadata_are_never_copied(self):
        """Output names come only from the registry, not source keys or metadata."""
        for kind, row in self.rows.items():
            with self.subTest(kind=kind):
                extra = {"__line__": 99, "meta": {"fixture": True}, "job_config": {"fixture": True},
                         "terminal_path": "fixture-only", "technical_overrides": {"fixture": True}}
                self.assertEqual(project_result_row(kind, SCHEMA_VERSION, row | extra), row)

    def test_missing_optional_null_blank_zero_and_sentinel_stay_distinct(self):
        """Legacy optional columns are omitted rather than filled or recalculated."""
        row = self.rows["triggers"].copy()
        del row["cell"]
        projected = project_result_row("triggers", SCHEMA_VERSION, row)
        self.assertNotIn("cell", projected)
        self.assertIsNone(projected["atr_short"])
        self.assertEqual(projected["bkt_short"], "")
        self.assertEqual(projected["p_long"], 0)
        self.assertEqual(projected["p_short"], "insufficient_bars")

    def test_unknown_kind_and_version_fail_without_echoing_inputs(self):
        """There is no generic passthrough for future or older unrecognized formats."""
        for kind, version in (("unknown-fixture", SCHEMA_VERSION), ("bb", "old-fixture"),
                              ("bb", "future-fixture"), ([], SCHEMA_VERSION)):
            with self.subTest(kind=kind, version=version):
                with self.assertRaisesRegex(ValueError, "^Unsupported result schema$"):
                    project_result_row(kind, version, self.rows["bb"])

    def test_required_column_absence_is_not_silently_adapted(self):
        """Older variants need a declared adapter rather than guessed field aliases."""
        for kind, row in self.rows.items():
            for field in RESULT_SCHEMAS[kind].required:
                with self.subTest(kind=kind, field=field):
                    incomplete = {key: value for key, value in row.items() if key != field}
                    with self.assertRaisesRegex(ValueError, "^Missing required result field$"):
                        project_result_row(kind, SCHEMA_VERSION, incomplete)

    def test_allowed_cells_reject_nested_and_nonfinite_values(self):
        """Allowed names cannot carry nested metadata, NaN or Infinity."""
        for value in ({"fixture": True}, [1], float("nan"), float("inf"), float("-inf")):
            with self.subTest(value=value):
                with self.assertRaisesRegex(ValueError, "^Invalid result cell$"):
                    project_result_row("bb", SCHEMA_VERSION, {"time": 100, "pct": value})
        with self.assertRaisesRegex(ValueError, "^Invalid result row$"):
            project_result_row("bb", SCHEMA_VERSION, [])

    def test_projection_does_not_mutate_input_or_round_values(self):
        """Caller-owned rows and original stored precision remain unchanged."""
        row = {"time": "1704067200", "pct": 0.123456789012345, "extra": "fixture"}
        projected = project_result_row("bb", SCHEMA_VERSION, row)
        self.assertEqual(projected, {"time": "1704067200", "pct": row["pct"]})
        projected["pct"] = 0
        self.assertEqual(row["pct"], 0.123456789012345)
        self.assertIn("extra", row)

    def test_registry_and_descriptors_are_immutable(self):
        """A caller cannot extend the global field allowlist or mutate coverage."""
        with self.assertRaises(TypeError):
            RESULT_SCHEMAS["fixture"] = get_result_schema("bb", SCHEMA_VERSION)
        with self.assertRaises(FrozenInstanceError):
            get_result_schema("bb", SCHEMA_VERSION).fields = ("fixture",)
        with self.assertRaises(FrozenInstanceError):
            self.descriptor().source_id = "other"

    def test_descriptor_serialization_is_explicit_and_preserves_timestamps(self):
        """No raw metadata can be injected or overwrite the field/schema selection."""
        descriptor = self.descriptor()
        output = descriptor.to_dict()
        self.assertEqual(set(output), {
            "dataset_id", "result_type", "schema_version", "source_id", "symbols", "timeframes",
            "coverage_start", "coverage_end", "generated_at", "fields",
        })
        self.assertEqual(output["fields"], ["time", "pct"])
        self.assertEqual(output["coverage_start"], "2024-01-01T00:00:00Z")
        output["symbols"].append("OTHER")
        self.assertEqual(descriptor.symbols, ("TEST",))
        with self.assertRaises(TypeError):
            self.descriptor(job_config={"fixture": True})

    def test_descriptor_rejects_paths_empty_or_duplicate_coverage(self):
        """Aliases are identifiers, not relative/absolute paths or hidden fallbacks."""
        for key, value in (
            ("dataset_id", "../fixture"), ("dataset_id", "C:\\fixture"),
            ("source_id", "fixture/source"), ("dataset_id", ""),
            ("source_id", "a" * 65), ("symbols", ()), ("symbols", ["TEST"]),
            ("symbols", ("TEST", "TEST")), ("symbols", ("../TEST",)),
            ("timeframes", ()), ("timeframes", ("M5", "M5")),
        ):
            with self.subTest(key=key, value=value):
                with self.assertRaises(ValueError):
                    self.descriptor(**{key: value})

    def test_descriptor_rejects_unknown_schema_and_invalid_time_coverage(self):
        """Dates must be ordered instants with explicit timezone information."""
        for changes in (
            {"schema_version": "unknown"}, {"result_type": "unknown"},
            {"coverage_start": "2024-02-01T00:00:00Z"},
            {"coverage_end": "2024-01-01"}, {"generated_at": "invalid-fixture"},
            {"generated_at": "2024-01-03T00:00:00"},
        ):
            with self.subTest(changes=changes):
                with self.assertRaises(ValueError):
                    self.descriptor(**changes)
        equivalent = replace(self.descriptor(), coverage_end="2023-12-31T19:00:00-05:00")
        self.assertEqual(equivalent.coverage_end, "2023-12-31T19:00:00-05:00")


if __name__ == "__main__":
    unittest.main()
