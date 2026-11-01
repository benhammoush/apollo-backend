"""Generic trusted-file CSV reads and conversions, independent of private writers.

These helpers preserve legacy values and companion metadata. They are not an
authorization boundary: visitor routes must still select/bound their own reads.
"""

import csv
import json
import logging
from pathlib import Path
import re
from typing import Any, Dict, List, Optional, Union

# Retain the existing logger identity for private callers' warning filters.
logger = logging.getLogger("shared.csv_utils")
_ISO_BARE_DT_RE = re.compile(r'^(\d{4}-\d{2}-\d{2})[T ](\d{2}:\d{2}:\d{2})$')
_DATETIME_COL_RE = re.compile(r"(?:^|_)(?:datetime|time|date|timestamp)$")


def read_csv_dataset(
    file_path: Path,
    return_metadata: bool = False,
) -> Union[List[Dict[str, str]], tuple[List[Dict[str, str]], Optional[Dict[str, Any]]]]:
    """Read legacy rows with line markers, width repair and optional companion data.

    Preserve blank-row skipping, first-warning behavior and metadata precedence;
    do not reinterpret these raw values or treat metadata as public disclosure.
    """
    rows = []
    with open(file_path, "r", newline="", encoding="utf-8-sig") as f:
        reader = csv.reader(f)
        try:
            headers = next(reader)
        except StopIteration:
            headers = None
        if headers is not None:
            num_header_fields = len(headers)
            line_num = 2
            short_warned = False
            long_warned = False
            for row in reader:
                if not row:
                    line_num += 1
                    continue
                if not any(field.strip() for field in row):
                    line_num += 1
                    logger.warning("%s line %d: all fields blank, skipping", file_path, line_num)
                    continue
                row_len = len(row)
                if row_len < num_header_fields:
                    if not short_warned:
                        logger.warning("%s line %d: row has %d fields (header has %d), padding with empty strings",
                                       file_path, line_num, row_len, num_header_fields)
                        short_warned = True
                    row.extend([""] * (num_header_fields - row_len))
                elif row_len > num_header_fields:
                    if not long_warned:
                        logger.warning("%s line %d: row has %d fields (header has %d), truncating",
                                       file_path, line_num, row_len, num_header_fields)
                        long_warned = True
                    row = row[:num_header_fields]
                row_dict: Dict[str, str] = {"__line__": str(line_num)}
                row_dict.update(dict(zip(headers, row)))
                rows.append(row_dict)
                line_num += 1
    if return_metadata:
        meta_path = file_path.with_suffix(".meta.json")
        metadata = None
        if meta_path.exists():
            with open(meta_path, "r", encoding="utf-8") as f:
                metadata = json.load(f)
        return rows, metadata
    return rows


def _is_datetime_col(key: str) -> bool:
    """Preserve exact/suffixed legacy datetime-name matching, not mask selection."""
    return bool(_DATETIME_COL_RE.search(key))


def convert_value_type(value: str, key: str) -> Any:
    """Preserve legacy line/date/scalar/JSON conversion ordering and missing values."""
    if key == "__line__":
        try:
            return int(value)
        except ValueError:
            return value
    if value == "":
        return None
    if value == "true":
        return True
    if value == "false":
        return False
    if _is_datetime_col(key):
        m = _ISO_BARE_DT_RE.match(value)
        if m:
            return f"{m.group(1)} {m.group(2)}+00:00"
        return value
    try:
        return int(value)
    except ValueError:
        pass
    try:
        return float(value)
    except ValueError:
        pass
    if (value.startswith("[") and value.endswith("]")) or (value.startswith("{") and value.endswith("}")):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            pass
    return value
