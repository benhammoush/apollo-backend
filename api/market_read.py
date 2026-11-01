"""Market worker transport/decoding primitives, without private route policy."""

import base64
from datetime import datetime
import io
import json

import httpx
import pandas as pd

_http_client: httpx.AsyncClient | None = None


def _get_client() -> httpx.AsyncClient:
    """Share persistent localhost worker connections with existing private reads."""
    global _http_client
    if _http_client is None or _http_client.is_closed:
        _http_client = httpx.AsyncClient(timeout=60.0)
    return _http_client


async def close_client():
    """Close transport after the owning host has drained its admitted reads."""
    global _http_client
    client, _http_client = _http_client, None
    if client is not None:
        await client.aclose()


def _decode_ohlcv_response(raw: bytes) -> list[dict]:
    """Preserve legacy JSON/parquet timestamps, numeric conversions and ordering."""
    data = json.loads(raw.decode("utf-8"))
    if data.get("format") == "parquet_b64":
        df = pd.read_parquet(io.BytesIO(base64.b64decode(data["data"])), engine="pyarrow")
        df["time"] = pd.to_datetime(df["time"], utc=True)
        bars_raw = df.to_dict(orient="records")
    else:
        bars_raw = data.get("bars", [])
    result = []
    for b in bars_raw:
        t = b["time"]
        if isinstance(t, str):
            t = int(datetime.fromisoformat(t.replace("Z", "+00:00")).timestamp())
        elif hasattr(t, "timestamp"):
            t = int(t.timestamp())
        result.append({"time": t, "open": float(b["open"]), "high": float(b["high"]),
                       "low": float(b["low"]), "close": float(b["close"]), "volume": float(b.get("volume", 0))})
    return result
