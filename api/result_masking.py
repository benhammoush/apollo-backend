"""Dates-only preview contract, shared by offline preparation and fail-closed reads."""

from datetime import date, datetime
import re

MASK = "xxxx"
MASK_VERSION = "apollo-dates-only-v1"
INDICATORS = frozenset({"Triggers_BT", "trends", "families", "brokers_historical_atr",
                        "brokers_live_spikedelay_live", "brokers_live_spreadpersession"})
_DATE_NAME = re.compile(r"(?:^|_)(?:datetime|timestamp|date|time)(?:_|$)", re.I)


def date_column_name(column: str) -> bool:
    """Recognize date-name candidates, never timeframe/duration values by their contents."""
    if (column.lower().startswith("time_to_hit")
            or re.search(r"(?:^|_)(?:duration|elapsed|latency|delay)(?:_|$)", column, re.I)):
        return False
    return bool(_DATE_NAME.search(column)) or column in {"created_at", "updated_at", "generated_at", "start", "end", "period"}


def validate_date_columns(columns: tuple[str, ...], dates: tuple[str, ...]) -> None:
    """Require an explicit unique date catalog drawn from approved date-named fields."""
    if (type(dates) is not tuple or any(type(name) is not str for name in dates)
            or len(set(dates)) != len(dates)
            or any(name not in columns or not date_column_name(name) for name in dates)):
        raise ValueError("Invalid retained date columns")


def valid_date(value) -> bool:
    """Allow whole ISO dates/datetimes/months or bounded Unix seconds/milliseconds only.

    Numeric epochs are restricted to 1990-2100; other numeric/text cells are not
    treated as dates merely because a permissive parser can interpret them.
    """
    if type(value) is not str or len(value) > 40:
        return False
    try:
        if re.fullmatch(r"\d{4}-\d{2}", value):
            date.fromisoformat(value + "-01")
            return True
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
            date.fromisoformat(value)
            return True
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?(?:Z|[+-]\d{2}:\d{2})?", value):
            datetime.fromisoformat(value.replace("Z", "+00:00"))
            return True
        if re.fullmatch(r"\d{9,10}(?:\.\d{1,6})?", value):
            return 631152000 <= float(value) <= 4102444800
        if re.fullmatch(r"\d{12,13}", value):
            return 631152000000 <= int(value) <= 4102444800000
    except (ValueError, OverflowError):
        return False
    return False


def masked_cells(columns: tuple[str, ...], cells: list[str], dates: tuple[str, ...]) -> list[str]:
    """Retain valid date strings exactly; even blanks, sentinels and nested values elsewhere mask."""
    retained = set(dates)
    return [value if name in retained and valid_date(value) else MASK
            for name, value in zip(columns, cells)]


def validate_masked_cells(columns: tuple[str, ...], cells: list, dates: tuple[str, ...]) -> None:
    """Reject raw/non-date values before any HTTP shape can serialize an artifact row."""
    retained = set(dates)
    if any(value != MASK and (name not in retained or not valid_date(value))
           for name, value in zip(columns, cells)):
        raise ValueError("Invalid masked artifact cells")
