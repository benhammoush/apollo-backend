"""Versioned display projections and full-column descriptors for selected results.

Pure projection and description only: no filesystem access, strategy imports,
dataset discovery, execution or publication. A schema is not approval to expose
a dataset; private operational readers retain their own contracts.
"""

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from math import isfinite
import re
from types import MappingProxyType
from typing import Any


SCHEMA_VERSION = "apollo-display-v1"
TABLE_SCHEMA_VERSION = "apollo-table-v2"
MAX_TABLE_COLUMNS = 4096


def validate_table_columns(columns: tuple[str, ...]) -> None:
    """Require an explicit ordered catalog; obvious sensitive/prototype fields fail closed."""
    sensitive = re.compile(r"password|passwd|secret|token|api.?key|account|login|username|email|terminal|(?:^|_)path(?:_|$)", re.I)
    if (type(columns) is not tuple or not 1 <= len(columns) <= MAX_TABLE_COLUMNS
            or any(type(column) is not str for column in columns) or len(set(columns)) != len(columns)):
        raise ValueError("Invalid table columns")
    for column in columns:
        if (type(column) is not str or re.fullmatch(r"[A-Za-z_][A-Za-z0-9_ .:()%+\-]{0,127}", column) is None
                or column in {"__proto__", "constructor", "prototype"} or sensitive.search(column)):
            raise ValueError("Invalid table column")


@dataclass(frozen=True)
class ResultSchema:
    """Immutable allowed field order and minimum legacy row shape."""

    fields: tuple[str, ...]
    required: tuple[str, ...]


# Only display/output fields; no strategy defaults, calculation dependencies or
# arbitrary companion metadata. Optional columns may be absent in older rows.
RESULT_SCHEMAS = MappingProxyType({
    "bb": ResultSchema(("time", "pct"), ("time", "pct")),
    "trends": ResultSchema(("time", "tf", "trend"), ("time", "tf", "trend")),
    "families": ResultSchema(
        ("date", "tf", "family_buy", "family_sell"),
        ("date", "tf", "family_buy", "family_sell"),
    ),
    "levels": ResultSchema(
        ("period", "level_type", "price", "time_from", "time_to", "color", "dashed"),
        ("period", "level_type", "price", "time_from", "time_to"),
    ),
    "session_levels": ResultSchema(
        ("date", "pair", "session", "start", "end", "open", "high", "low", "close",
         "avg_50", "range", "direction", "range_pips"),
        ("date", "pair", "session", "start", "end", "open", "high", "low", "close"),
    ),
    "triggers": ResultSchema(
        ("datetime", "row_type", "pair", "timeframe", "dir", "entry", "bb_trigger",
         "atr_short", "atr_long", "ref_short", "ref_long", "p_short", "p_long",
         "bkt_short", "bkt_long", "cell"),
        ("datetime", "pair", "timeframe", "dir", "entry"),
    ),
    "divergences": ResultSchema(
        ("divergence_datetime", "pair", "timeframe", "dir", "divergence_close",
         "divergence_bb", "pivot_bb_datetime", "pivot_price", "pivot_price_datetime"),
        ("divergence_datetime", "pair", "timeframe", "dir", "divergence_close"),
    ),
    "pivots": ResultSchema(
        ("pivot_bb_datetime", "pair", "timeframe", "dir", "pivot_bb", "pivot_price",
         "pivot_price_datetime", "pivot_close", "pivot_close_datetime",
         "date_suppression", "type_suppression"),
        ("pivot_bb_datetime", "pair", "timeframe", "dir", "pivot_price"),
    ),
})


def get_result_schema(result_type: str, schema_version: str) -> ResultSchema:
    """Reject unknown kinds/versions instead of guessing an old output schema."""
    if type(result_type) is not str or schema_version != SCHEMA_VERSION:
        raise ValueError("Unsupported result schema")
    schema = RESULT_SCHEMAS.get(result_type)
    if schema is None:
        raise ValueError("Unsupported result schema")
    return schema


def project_result_row(
    result_type: str, schema_version: str, row: Mapping[str, Any]
) -> dict[str, Any]:
    """Copy allowed scalar cells without conversion, rounding or missing-value fill.

    Missing optional keys stay absent; explicit null, blank and semantic-sentinel
    cells retain their values. Required-key absence or non-JSON scalar cells fail
    closed. This is field projection, not full semantic validation of cell values.
    """
    schema = get_result_schema(result_type, schema_version)
    if not isinstance(row, Mapping):
        raise ValueError("Invalid result row")
    if any(field not in row for field in schema.required):
        raise ValueError("Missing required result field")
    result = {}
    for field in schema.fields:
        if field not in row:
            continue
        value = row[field]
        if type(value) not in (str, int, float, bool, type(None)):
            raise ValueError("Invalid result cell")
        if type(value) is float and not isfinite(value):
            raise ValueError("Invalid result cell")
        result[field] = value
    return result


def _aware_datetime(value: str) -> datetime:
    """Validate an explicit timestamp without changing its original representation."""
    if type(value) is not str:
        raise ValueError("Invalid dataset timestamp")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.utcoffset() is None:
            raise ValueError
    except (ValueError, OverflowError):
        raise ValueError("Invalid dataset timestamp") from None
    return parsed


@dataclass(frozen=True)
class DatasetDescriptor:
    """Explicit public aliases/coverage, never copied raw result-file metadata.

    Construction validates structure only. Dataset selection, redistribution
    approval, storage containment and matching row coverage belong to the later
    operator/read boundary, not to this description object.
    """

    dataset_id: str
    result_type: str
    schema_version: str
    source_id: str
    symbols: tuple[str, ...]
    timeframes: tuple[str, ...]
    coverage_start: str | None
    coverage_end: str | None
    generated_at: str
    columns: tuple[str, ...] = ()
    revision: str | None = None
    row_count: int | None = None
    date_columns: tuple[str, ...] | None = None
    indicator: str | None = None
    filename: str | None = None

    def __post_init__(self):
        """Validate schema, path-free aliases, explicit coverage and timestamps."""
        masked = self.date_columns is not None
        if masked:
            from api.result_masking import INDICATORS, validate_date_columns
            if self.schema_version != TABLE_SCHEMA_VERSION or self.indicator not in INDICATORS:
                raise ValueError("Invalid masked preview")
            if self.symbols or self.timeframes:
                raise ValueError("Masked previews do not disclose row dimensions")
            validate_date_columns(self.columns, self.date_columns)
            if (type(self.filename) is not str
                    or re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,119}\.csv", self.filename) is None
                    or ".." in self.filename):
                raise ValueError("Invalid public filename")
        elif self.indicator is not None or self.filename is not None:
            raise ValueError("Unexpected masked preview fields")
        if self.schema_version == TABLE_SCHEMA_VERSION:
            if self.result_type != "table":
                raise ValueError("Unsupported table result type")
            validate_table_columns(self.columns)
            if type(self.revision) is not str or re.fullmatch(r"[0-9a-f]{64}", self.revision) is None:
                raise ValueError("Invalid table revision")
            if type(self.row_count) is not int or not 0 <= self.row_count <= 2_000_000:
                raise ValueError("Invalid table row count")
        else:
            get_result_schema(self.result_type, self.schema_version)
            if self.columns or self.revision is not None or self.row_count is not None:
                raise ValueError("Unexpected legacy table fields")
        for value in (self.dataset_id, self.source_id):
            if type(value) is not str or re.fullmatch(r"[a-z0-9][a-z0-9_-]{0,63}", value) is None:
                raise ValueError("Invalid dataset alias")
        for values in (self.symbols, self.timeframes):
            if type(values) is not tuple or (not values and not masked):
                raise ValueError("Invalid dataset coverage")
            if any(type(value) is not str or re.fullmatch(r"[A-Za-z0-9_]{1,32}", value) is None
                   for value in values):
                raise ValueError("Invalid dataset coverage")
            if len(set(values)) != len(values):
                raise ValueError("Invalid dataset coverage")
        start = _aware_datetime(self.coverage_start) if self.coverage_start is not None or not masked else None
        end = _aware_datetime(self.coverage_end) if self.coverage_end is not None or not masked else None
        _aware_datetime(self.generated_at)
        if (start is None) != (end is None) or (start is not None and end < start):
            raise ValueError("Invalid dataset coverage")

    def to_dict(self) -> dict[str, Any]:
        """Serialize exactly the descriptor contract, including schema field order."""
        result = {
            "dataset_id": self.dataset_id,
            "result_type": self.result_type,
            "schema_version": self.schema_version,
            "source_id": self.source_id,
            "symbols": list(self.symbols),
            "timeframes": list(self.timeframes),
            "coverage_start": self.coverage_start,
            "coverage_end": self.coverage_end,
            "generated_at": self.generated_at,
            "fields": list(self.columns if self.schema_version == TABLE_SCHEMA_VERSION
                           else get_result_schema(self.result_type, self.schema_version).fields),
        }
        if self.schema_version == TABLE_SCHEMA_VERSION:
            result.update({"revision": self.revision, "total": self.row_count, "paginated": True})
        if self.date_columns is not None:
            result.update({"masked_preview": True, "retained_date_columns": list(self.date_columns),
                           "indicator": self.indicator, "filename": self.filename,
                            "data_notice": (("Template preview: chronological synthetic dates; all other cells are xxxx. No original rows."
                                if self.date_columns else
                                "Template preview: generated placeholder rows; every cell is xxxx, including dates. No original rows.")
                                if self.source_id == "template-preview" else
                                "Placeholder-only preview: original headers; every cell is xxxx, including dates. No original row values are shown."
                                if not self.date_columns else
                                "Masked preview: original headers and valid dates only; other cells are xxxx.")})
        return result
