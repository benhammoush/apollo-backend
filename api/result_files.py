"""Legacy saved-result readers, independent of agent execution and strategy imports.

These internal helpers preserve existing CSV/metadata behavior. Callers own path
selection; this module is not a public dataset allowlist or authorization boundary.
"""

import json
import threading
import time as _time
from pathlib import Path


_CSV_CACHE_TTL = 300  # seconds
_csv_cache: dict[str, tuple[list[dict], float]] = {}
_csv_cache_lock = threading.Lock()


def _load_csv_file(path: Path) -> list[dict]:
    """Load parsed CSV rows from disk (or the shared thread-safe legacy cache)."""
    from shared.csv_read import read_csv_dataset as _read_ds

    key = str(path)
    with _csv_cache_lock:
        entry = _csv_cache.get(key)
        if entry is not None and (_time.monotonic() - entry[1]) < _CSV_CACHE_TTL:
            return entry[0]

    data = _read_ds(path)

    with _csv_cache_lock:
        _csv_cache[key] = (data, _time.monotonic())

    return data


def _load_and_filter_bb(
    dir_path: Path, symbol: str, series_key: str, start_ts: int | None, end_ts: int | None
) -> list:
    """Read stored BB rows using the existing inclusive timestamp bounds."""
    tf_suffix = series_key.removeprefix("series_")
    csv_path = dir_path / f"{symbol}_{tf_suffix}.csv"
    if not csv_path.exists():
        return []
    rows = _load_csv_file(csv_path)
    if start_ts is not None:
        filtered = []
        for pt in rows:
            try:
                time_val = int(pt["time"])
            except (ValueError, TypeError, KeyError):
                continue
            if time_val >= start_ts:
                filtered.append(pt)
        rows = filtered
    if end_ts is not None:
        filtered = []
        for pt in rows:
            try:
                time_val = int(pt["time"])
            except (ValueError, TypeError, KeyError):
                continue
            if time_val <= end_ts:
                filtered.append(pt)
        rows = filtered
    return rows


def _find_data_file(dir_path: Path, symbol: str) -> tuple[str | None, Path | None, list[Path]]:
    """Find legacy symbol CSVs, preferring the simple file over BB series files."""
    simple = dir_path / f"{symbol}.csv"
    if simple.is_file():
        return ("simple", simple, [simple])

    bb_files = sorted(dir_path.glob(f"{symbol}_*.csv"))
    if bb_files:
        return ("bb", bb_files[0], bb_files)

    return (None, None, [])


def _read_bb_data(dir_path: Path, symbol: str) -> dict:
    """Combine legacy BB series and companion metadata without recalculation."""
    result: dict = {}
    for tf_file in sorted(dir_path.glob(f"{symbol}_*.csv")):
        stem = tf_file.stem
        suffix = stem[len(symbol) + 1:]
        result[f"series_{suffix}"] = _load_csv_file(tf_file)
    meta_path = dir_path / f"{symbol}.meta.json"
    if meta_path.exists():
        with open(meta_path, encoding="utf-8") as f:
            result["meta"] = json.load(f)
    return result
