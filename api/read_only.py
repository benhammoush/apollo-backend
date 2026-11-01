"""Default-deny visitor application, separate from the operational Apollo app.

Only explicit market reads, selected results and filtered market events are
available by default. An optional server-bound, fixed-target MT5 bootstrap is a
separate narrow capability; it never grants generic worker/job management.
"""

import asyncio
from collections import deque
from contextlib import suppress
import csv
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
import json
from math import isfinite
from pathlib import Path
from types import MappingProxyType
from typing import Any
from urllib.parse import urlsplit

from fastapi import FastAPI, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, Response
from starlette.exceptions import HTTPException as StarletteHTTPException
from starlette.datastructures import MutableHeaders
from starlette.routing import Match

from api.result_contracts import DatasetDescriptor, TABLE_SCHEMA_VERSION, project_result_row
from api.read_only_storage import SelectedStorage
from api.market_bootstrap import MarketFeedBootstrap


_OVERLAYS = {"bb": ("bb", "data"), "trends": ("trends", "changes"),
             "families": ("families", "families"), "triggers": ("Triggers_BT", "triggers"),
             "session_levels": ("session_levels", "rows")}

_METADATA_ROUTES = frozenset({
    "/capabilities", "/health", "/pairs", "/data/brokers", "/public/results",
    "/agents/folders/list", "/storage/tree", "/storage/meta",
    "/public/market-feed",
})
_WORK_ROUTES = frozenset({
    "/market/{symbol}", "/public/results/{dataset_id}",
    "/agents/{agent}/folders/{dataset_id}/{symbol}/data", "/agents/bb/{symbol}/series",
    "/storage/file",
    "/public/market-feed/start",
})
# The ngrok warning bypass is nonsecret transport metadata, never authorization.
_CORS_HEADERS = frozenset({"accept", "accept-language", "content-language", "content-type",
                           "x-query-trigger", "x-query-interval", "ngrok-skip-browser-warning"})


@dataclass(frozen=True)
class SelectedResult:
    """Operator-selected result artifact and public description; never visitor paths."""

    description: DatasetDescriptor
    path: Path
    snapshot_identity: tuple | None = field(default=None, init=False, repr=False)

    def __post_init__(self):
        """Pin indexed artifacts for this process; changed files require reselection."""
        if self.description.schema_version == TABLE_SCHEMA_VERSION:
            from api.result_snapshot import file_identity
            object.__setattr__(self, "snapshot_identity", file_identity(Path(self.path)))


@dataclass(frozen=True)
class ReadOnlyPolicy:
    """Explicit selection and conservative local guardrails, not measured capacity."""

    symbols: tuple[str, ...]
    sources: dict[str, str]
    origins: tuple[str, ...]
    result_root: Path
    results: tuple[SelectedResult, ...] = ()
    history_days: dict[str, int] = field(default_factory=lambda: {
        # Original chart windows subtract 730/1825 days and request both end
        # dates inclusively. Keep those windows bounded without truncation.
        "M1": 7, "M5": 60, "M15": 240, "H1": 731, "H4": 1826,
        "D1": 7300, "W1": 7300, "MN1": 7300,
    })
    max_rows: int = 25000
    max_bytes: int = 8_000_000
    max_file_bytes: int = 10_000_000
    max_scan_rows: int = 100000
    max_offset: int = 100000
    max_inflight: int = 2
    max_metadata_inflight: int = 16
    request_timeout: int = 60
    max_connections: int = 20
    max_subscriptions: int = 8
    ws_queue_size: int = 32
    fixture: bool = False
    masked_results_only: bool = False
    market_start_enabled: bool = False

    def __post_init__(self):
        """Reject incomplete/wildcard selections and freeze mutable configuration."""
        if not self.symbols or len(set(self.symbols)) != len(self.symbols):
            raise ValueError("Explicit unique public symbols required")
        if any(not isinstance(s, str) or not s.replace("_", "").isalnum() for s in self.symbols):
            raise ValueError("Invalid public symbol")
        if not self.sources or len(set(self.sources.values())) != len(self.sources):
            raise ValueError("Explicit unique public sources required")
        for alias, source in self.sources.items():
            if not isinstance(alias, str) or not alias.replace("-", "").replace("_", "").isalnum():
                raise ValueError("Invalid public source alias")
            if not isinstance(source, str) or not source:
                raise ValueError("Invalid public source")
        if not self.origins:
            raise ValueError("Explicit public origins required")
        for origin in self.origins:
            parts = urlsplit(origin)
            if parts.scheme not in {"http", "https"} or not parts.netloc or parts.path or parts.query or parts.fragment or parts.username or parts.password:
                raise ValueError("Invalid public origin")
        for name in ("max_rows", "max_bytes", "max_file_bytes", "max_scan_rows", "max_offset",
                     "max_inflight", "max_metadata_inflight", "request_timeout", "max_connections", "max_subscriptions", "ws_queue_size"):
            if type(getattr(self, name)) is not int or getattr(self, name) < 1:
                raise ValueError("Invalid public limit")
        if not self.history_days or any(type(v) is not int or v < 1 for v in self.history_days.values()):
            raise ValueError("Explicit history limits required")
        if type(self.masked_results_only) is not bool:
            raise ValueError("Invalid masked preview profile")
        if type(self.market_start_enabled) is not bool:
            raise ValueError("Invalid market bootstrap capability")
        ids, handles = set(), set()
        for selection in self.results:
            d = selection.description
            if d.dataset_id in ids or d.source_id not in self.sources or not set(d.symbols).issubset(self.symbols):
                raise ValueError("Invalid selected result")
            ids.add(d.dataset_id)
            if self.masked_results_only and d.date_columns is None:
                raise ValueError("Only masked result selections allowed")
            if d.date_columns is not None and not self.masked_results_only:
                raise ValueError("Masked selections require the explicit masked-only profile")
            handle = (f"indicators/{d.indicator}/{d.filename}" if d.date_columns is not None
                      else f"indicators/{d.dataset_id}/data.csv").casefold()
            if handle in handles:
                raise ValueError("Duplicate public file handle")
            handles.add(handle)
        object.__setattr__(self, "sources", MappingProxyType(dict(self.sources)))
        object.__setattr__(self, "history_days", MappingProxyType(dict(self.history_days)))
        object.__setattr__(self, "result_root", Path(self.result_root).resolve())


def validate_market_start_binding(policy: ReadOnlyPolicy, market_starter: MarketFeedBootstrap | None) -> None:
    """Keep worker lifecycle control outside the browser-facing visitor app."""
    if policy.market_start_enabled or market_starter is not None:
        raise ValueError("Worker startup is Windows-local and cannot be bound to the visitor API")


def _safe_error(status: int) -> JSONResponse:
    """Return no raw input, exception detail, paths, configuration or diagnostics."""
    return JSONResponse({"error": "read_only_request_unavailable"}, status_code=status,
                        headers={"Cache-Control": "no-store", "Retry-After": "3"})


def _scalar_fields(value: dict, fields: tuple[str, ...]) -> dict:
    """Allow named JSON scalar fields only, without numeric or timestamp coercion."""
    output = {}
    for key in fields:
        if key in value:
            cell = value[key]
            if type(cell) not in (str, int, float, bool, type(None)) or (type(cell) is float and not isfinite(cell)):
                raise ValueError("Invalid market cell")
            output[key] = cell
    return output


class ReadOnlyApplication:
    """ASGI visitor boundary with no generic forwarding to operational HTTP routes."""

    def __init__(self, policy: ReadOnlyPolicy, services: Any, websocket_app: Any,
                 market_starter: MarketFeedBootstrap | None = None):
        """Inject shared market services and the existing market-event producer app."""
        self.policy = policy
        self.services = services
        self.websocket_app = websocket_app
        validate_market_start_binding(policy, market_starter)
        self.market_starter = market_starter
        self._active = {"metadata": 0, "work": 0}
        self._connections = 0
        self._arrivals = {"metadata": deque(), "work": deque()}
        self._http_tasks = set()
        self._closing = False
        self.http = FastAPI(docs_url=None, redoc_url=None, openapi_url=None, redirect_slashes=False)
        self._register_http()
        if any(route.path not in _METADATA_ROUTES | _WORK_ROUTES for route in self.http.routes):
            raise ValueError("Public route admission classification required")

    def _admission_lane(self, scope) -> str | None:
        """Classify selected reads; private paths/unselected symbols use no quota."""
        for route in self.http.routes:
            matched, child_scope = route.matches(scope)
            if matched is Match.FULL:
                symbol = child_scope.get("path_params", {}).get("symbol")
                if symbol is not None and symbol not in self.policy.symbols:
                    return None
                if route.path in _METADATA_ROUTES:
                    return "metadata"
                if route.path in _WORK_ROUTES:
                    return "work"
                return None
        return None

    def _json(self, value: Any) -> JSONResponse:
        """Bound serialized response bytes before starting the HTTP response."""
        response = JSONResponse(value, headers={"Cache-Control": "no-store"})
        if len(response.body) > self.policy.max_bytes:
            raise HTTPException(413)
        return response

    async def _serve_http(self, scope, receive, send):
        """Keep admission until both the real handler work and HTTP waiter finish.

        Response timeouts and disconnected clients must not cancel a handler that
        is awaiting executor work: cancelling to_thread cannot stop its thread.
        Timed-out handlers remain charged, with bounded buffered output and no
        late response sent to the expired caller.
        """
        if self._closing:
            await _safe_error(503)(scope, receive, send)
            return
        request = Request(scope)
        if request.method not in {"GET", "OPTIONS"}:
            await _safe_error(403)(scope, receive, send)
            return
        if request.headers.get("transfer-encoding") or request.headers.get("content-length") not in (None, "0"):
            await _safe_error(413)(scope, receive, send)
            return
        if request.headers.get("origin") not in (None, *self.policy.origins):
            await _safe_error(403)(scope, receive, send)
            return
        if request.method == "OPTIONS":
            # Match only a registered GET without invoking its handler or workers.
            requested = request.headers.get("access-control-request-headers", "")
            headers = {header.strip().lower() for header in requested.split(",")} if requested else set()
            if (request.headers.get("origin") not in self.policy.origins
                    or request.headers.get("access-control-request-method") != "GET"
                    or request.headers.get("access-control-request-private-network") is not None
                    or not headers.issubset(_CORS_HEADERS)
                    or self._admission_lane(dict(scope, method="GET")) is None):
                await _safe_error(403)(scope, receive, send)
                return
            await Response(status_code=204, headers={
                "Cache-Control": "no-store", "Access-Control-Allow-Methods": "GET",
                "Access-Control-Allow-Headers": ", ".join(sorted(_CORS_HEADERS)),
                "Access-Control-Max-Age": "600",
                "Vary": "Access-Control-Request-Method, Access-Control-Request-Headers",
            })(scope, receive, send)
            return
        lane = self._admission_lane(scope)
        if lane is None:
            await _safe_error(404)(scope, receive, send)
            return
        now = asyncio.get_running_loop().time()
        arrivals = self._arrivals[lane]
        while arrivals and arrivals[0] < now - 1:
            arrivals.popleft()
        limit = self.policy.max_metadata_inflight if lane == "metadata" else self.policy.max_inflight
        if len(request.url.query) > 2048 or len(arrivals) >= 20 or self._active[lane] >= limit:
            await _safe_error(429)(scope, receive, send)
            return
        arrivals.append(now)
        self._active[lane] += 1
        finished = {"work": False, "response": False}
        released = False

        def release_if_finished():
            """Return a slot exactly once, including after late executor completion."""
            nonlocal released
            if not released and all(finished.values()):
                released = True
                self._active[lane] -= 1

        async def collect_response():
            """Buffer only bounded completed responses, never a timed-out late send."""
            messages = []
            size = 0
            budget = max(self.policy.max_bytes, len(_safe_error(503).body))

            async def buffer(message):
                """Bound body bytes and event count before retaining ASGI output."""
                nonlocal size
                if message["type"] not in {"http.response.start", "http.response.body"}:
                    raise ValueError("Unexpected HTTP response event")
                size += len(message.get("body", b""))
                if size > budget or len(messages) >= 64:
                    raise ValueError("HTTP response budget exceeded")
                messages.append(dict(message))

            await self.http(scope, receive, buffer)
            return messages

        def completed(task):
            """Retire real work even when no HTTP caller is waiting for its result."""
            self._http_tasks.discard(task)
            finished["work"] = True
            if not task.cancelled():
                task.exception()
            release_if_finished()

        task = asyncio.create_task(collect_response())
        self._http_tasks.add(task)
        task.add_done_callback(completed)
        try:
            try:
                messages = await asyncio.wait_for(asyncio.shield(task), timeout=self.policy.request_timeout)
            except Exception:
                await _safe_error(503)(scope, receive, send)
            else:
                for message in messages:
                    await send(message)
        finally:
            finished["response"] = True
            release_if_finished()

    def seal(self):
        """Stop new HTTP/WS admission before the listener begins shutting down."""
        self._closing = True

    async def close(self):
        """Join detached timed-out/disconnected handlers without cancelling threads."""
        self.seal()
        pending = tuple(self._http_tasks)
        interrupted = False
        if pending:
            drain = asyncio.gather(*pending, return_exceptions=True)
            while True:
                try:
                    await asyncio.shield(drain)
                    break
                except asyncio.CancelledError:
                    interrupted = True
        if interrupted:
            raise asyncio.CancelledError

    def _query(self, request: Request, allowed: set[str]) -> dict[str, str]:
        """Reject unknown/duplicate query keys rather than accepting hidden options."""
        pairs = list(request.query_params.multi_items())
        if len({key for key, _ in pairs}) != len(pairs) or any(key not in allowed for key, _ in pairs):
            raise HTTPException(422)
        return dict(pairs)

    def _source(self, alias: str | None) -> tuple[str, str]:
        """Default only to the first explicitly selected source, never registry fallback."""
        alias = alias if alias is not None else next(iter(self.policy.sources))
        if alias not in self.policy.sources:
            raise HTTPException(404)
        return alias, self.policy.sources[alias]

    def _symbol(self, symbol: str) -> str:
        """Require an exact configured symbol; do not discover arbitrary instruments."""
        if symbol not in self.policy.symbols:
            raise HTTPException(404)
        return symbol

    def _dates(self, query: dict[str, str], tf: str) -> tuple[str, str]:
        """Validate inclusive calendar-date budgets, preserving existing route semantics."""
        if tf not in self.policy.history_days:
            raise HTTPException(422)
        days = self.policy.history_days[tf]
        try:
            end = datetime.strptime(query["end"], "%Y-%m-%d").date() if query.get("end") else datetime.now(timezone.utc).date()
            start = datetime.strptime(query["start"], "%Y-%m-%d").date() if query.get("start") else end - timedelta(days=days - 1)
        except ValueError:
            raise HTTPException(422) from None
        if start > end or (end - start).days + 1 > days:
            raise HTTPException(422)
        return start.isoformat(), end.isoformat()

    def _selection(self, dataset_id: str) -> SelectedResult:
        """Resolve only a selected public ID, with no path or latest-folder fallback."""
        for result in self.policy.results:
            if result.description.dataset_id == dataset_id:
                return result
        raise HTTPException(404)

    def _result_rows(self, selection: SelectedResult, offset: int, limit: int,
                     start_ts: float | None = None, end_ts: float | None = None) -> tuple[list, int]:
        """Read a bounded legacy CSV or indexed page without companion metadata."""
        if selection.description.schema_version == TABLE_SCHEMA_VERSION:
            if start_ts is not None or end_ts is not None:
                raise HTTPException(422)
            from api.result_snapshot import read_snapshot
            return read_snapshot(selection, self.policy, offset, limit)
        from shared.csv_read import convert_value_type

        path = Path(selection.path).resolve()
        if not path.is_relative_to(self.policy.result_root) or path.suffix.lower() != ".csv":
            raise HTTPException(404)
        # Filesystem writers are trusted operators. Atomic publication/updates
        # and selection identity remain separate gates; visitors cannot set paths.
        with path.open("rb") as stream:
            raw = stream.read(self.policy.max_file_bytes + 1)
        if len(raw) > self.policy.max_file_bytes:
            raise HTTPException(413)
        import io
        reader = csv.DictReader(io.StringIO(raw.decode("utf-8-sig")))
        if reader.fieldnames is None or len(set(reader.fieldnames)) != len(reader.fieldnames):
            raise HTTPException(422)
        from api.result_contracts import get_result_schema
        schema = get_result_schema(selection.description.result_type, selection.description.schema_version)
        if not set(schema.required).issubset(reader.fieldnames):
            raise HTTPException(422)
        rows = []
        total = 0
        d = selection.description
        for scanned, raw_row in enumerate(reader):
            if scanned >= self.policy.max_scan_rows:
                raise HTTPException(413)
            row = {key: convert_value_type(value, key) for key, value in raw_row.items() if key is not None and value is not None}
            projected = project_result_row(d.result_type, d.schema_version, row)
            if "pair" in projected and projected["pair"] not in d.symbols:
                raise HTTPException(422)
            if start_ts is not None or end_ts is not None:
                key = {"bb": "time", "trends": "time", "families": "date", "session_levels": "date",
                       "triggers": "datetime", "divergences": "divergence_datetime", "pivots": "pivot_bb_datetime"}.get(d.result_type)
                value = projected.get(key)
                try:
                    try:
                        ts = float(value)
                    except (ValueError, TypeError):
                        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
                        ts = (dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)).timestamp()
                    if not isfinite(ts):
                        raise ValueError
                except (AttributeError, ValueError, TypeError, OverflowError):
                    raise HTTPException(422) from None
                if (start_ts is not None and ts < start_ts) or (end_ts is not None and ts > end_ts):
                    continue
            if offset <= total < offset + limit:
                rows.append(projected)
            total += 1
        return rows, total

    def _register_http(self):
        """Register explicit read handlers only; no operational router is mounted."""
        app = self.http

        @app.exception_handler(StarletteHTTPException)
        async def http_error(request, exc):
            """Sanitize all router errors, including unknown paths/methods."""
            return _safe_error(exc.status_code)

        @app.exception_handler(RequestValidationError)
        async def validation_error(request, exc):
            """Do not echo invalid inputs or schema internals to visitors."""
            return _safe_error(422)

        @app.get("/capabilities")
        async def capabilities(request: Request):
            """Advertise server-owned mode, not a visitor-selected role/profile."""
            self._query(request, set())
            return self._json({"read_only": True, "fixture": self.policy.fixture})

        @app.get("/public/market-feed")
        async def market_feed_status(request: Request):
            """Report that browser clients cannot start or control workers."""
            self._query(request, set())
            return self._json({"enabled": False, "state": "disabled", "can_start": False, "retry_after_seconds": 0})

        @app.get("/health")
        async def health(request: Request):
            """Report visitor-listener liveness only, not worker/process diagnostics."""
            self._query(request, set())
            return self._json({"status": "ok", "read_only": True})

        @app.get("/pairs")
        async def pairs(request: Request):
            """Expose selected symbol names only."""
            self._query(request, set())
            return self._json([{"symbol": symbol} for symbol in self.policy.symbols])

        @app.get("/data/brokers")
        async def sources(request: Request):
            """Expose selected aliases only for available exact-match history sources."""
            self._query(request, set())
            available = await self.services.available_sources()
            return self._json({"brokers": [alias for alias, source in self.policy.sources.items() if source in available]})

        @app.get("/market/{symbol}")
        async def market(symbol: str, request: Request):
            """Serve bounded selected-source OHLCV through shared serialized MT5 services."""
            q = self._query(request, {"tf", "source", "start", "end"})
            _, source = self._source(q.get("source"))
            tf = q.get("tf", "H1")
            start, end = self._dates(q, tf)
            rows = await self.services.market(self._symbol(symbol), tf, source, start, end)
            if len(rows) > self.policy.max_rows:
                raise HTTPException(413)
            return self._json([_scalar_fields(row, ("time", "open", "high", "low", "close", "volume")) for row in rows])

        @app.get("/public/results")
        async def catalog(request: Request):
            """List only explicitly selected public descriptors, never cache/storage trees."""
            self._query(request, set())
            return self._json([r.description.to_dict() for r in self.policy.results])

        @app.get("/public/results/{dataset_id}")
        async def result(dataset_id: str, request: Request):
            """Return selected projected result rows with bounded pagination."""
            q = self._query(request, {"offset", "limit", "revision"})
            selection = self._selection(dataset_id)
            paged = selection.description.schema_version == TABLE_SCHEMA_VERSION
            if paged:
                from api.result_snapshot import page_size
                if q.get("revision") != selection.description.revision:
                    raise HTTPException(409)
            elif "revision" in q:
                raise HTTPException(422)
            try:
                offset = int(q.get("offset", "0"))
                limit = int(q.get("limit", str(page_size(selection.description, self.policy) if paged else 500)))
            except ValueError:
                raise HTTPException(422) from None
            if not 0 <= offset <= (selection.description.row_count if paged else self.policy.max_offset) or not 1 <= limit <= self.policy.max_rows:
                raise HTTPException(422)
            rows, total = await asyncio.to_thread(self._result_rows, selection, offset, limit)
            return self._json({"rows": rows, "total": total, "offset": offset, "limit": limit,
                               "dataset": selection.description.to_dict()})

        @app.get("/agents/folders/list")
        async def overlay_catalog(request: Request):
            """Adapt selected dataset IDs to the existing chart source-selector shape."""
            self._query(request, set())
            return self._json([
                {"name": r.description.dataset_id, "source": "Selected result",
                 "created_at": r.description.generated_at,
                 "agents": [_OVERLAYS[r.description.result_type][0]],
                 "brokers": {_OVERLAYS[r.description.result_type][0]: r.description.source_id}}
                for r in self.policy.results if r.description.result_type in _OVERLAYS
                and len(r.description.symbols) == 1
            ])

        @app.get("/storage/tree")
        async def storage_tree(request: Request):
            """Serve selected virtual handles in the original Storage tree shape."""
            self._query(request, set())
            return self._json(SelectedStorage(self.policy, self._result_rows).tree())

        @app.get("/storage/meta")
        async def storage_meta(request: Request):
            """Supply only selected descriptor fields to the original metadata viewer."""
            q = self._query(request, {"path"})
            if "path" not in q:
                raise HTTPException(422)
            return self._json(SelectedStorage(self.policy, self._result_rows).metadata(q["path"]))

        @app.get("/storage/file")
        async def storage_file(request: Request):
            """Serve bounded projected columnar JSON or its CSV compatibility fallback."""
            q = self._query(request, {"path", "format", "offset", "limit", "revision"})
            if "path" not in q or q.get("format", "csv") not in {"csv", "json"}:
                raise HTTPException(422)
            try:
                offset, limit = int(q.get("offset", "0")), int(q.get("limit", "0"))
            except ValueError:
                raise HTTPException(422) from None
            storage = SelectedStorage(self.policy, self._result_rows)
            selected = storage.selection(q["path"])
            if selected.description.schema_version == TABLE_SCHEMA_VERSION:
                if q.get("revision") != selected.description.revision:
                    raise HTTPException(409)
                if q.get("format", "csv") != "json":
                    raise HTTPException(422)
            elif "revision" in q:
                raise HTTPException(422)
            page = await asyncio.to_thread(storage.page, q["path"], offset, limit)
            if q.get("format", "csv") == "json":
                return self._json(page)
            body = await asyncio.to_thread(storage.csv, page)
            return Response(body, media_type="text/csv", headers={"Cache-Control": "no-store"})

        @app.get("/agents/{agent}/folders/{dataset_id}/{symbol}/data")
        async def overlay(agent: str, dataset_id: str, symbol: str, request: Request):
            """Serve existing overlays from selected files, never arbitrary agent folders."""
            q = self._query(request, {"broker", "start", "end", "offset"})
            r = self._selection(dataset_id)
            d = r.description
            if d.result_type not in _OVERLAYS or _OVERLAYS[d.result_type][0] != agent or d.symbols != (symbol,):
                raise HTTPException(404)
            if q.get("broker", d.source_id) != d.source_id or q.get("offset", "0") != "0":
                raise HTTPException(422)
            try:
                start_ts = float(q["start"]) if "start" in q else None
                end_ts = float(q["end"]) if "end" in q else None
                if any(v is not None and not isfinite(v) for v in (start_ts, end_ts)):
                    raise ValueError
                if start_ts is not None and end_ts is not None and end_ts < start_ts:
                    raise ValueError
            except ValueError:
                raise HTTPException(422) from None
            rows, total = await asyncio.to_thread(self._result_rows, r, 0, self.policy.max_rows, start_ts, end_ts)
            if total > self.policy.max_rows:
                raise HTTPException(413)
            return self._json({_OVERLAYS[d.result_type][1]: rows})

        @app.get("/agents/bb/{symbol}/series")
        async def bb_series(symbol: str, request: Request):
            """Use an explicitly selected BB series instead of latest/raw-folder lookup."""
            q = self._query(request, {"folder", "tf", "start", "end"})
            tf = q.get("tf", "M15")
            start, end = self._dates(q, tf)
            candidates = [r for r in self.policy.results if r.description.result_type == "bb"
                          and r.description.symbols == (symbol,) and tf in r.description.timeframes
                          and ("folder" not in q or r.description.dataset_id == q["folder"])]
            if len(candidates) != 1:
                raise HTTPException(404)
            start_ts = datetime.fromisoformat(start).replace(tzinfo=timezone.utc).timestamp()
            end_ts = datetime.fromisoformat(end).replace(tzinfo=timezone.utc).timestamp() + 86400 - 1
            rows, total = await asyncio.to_thread(self._result_rows, candidates[0], 0, self.policy.max_rows, start_ts, end_ts)
            if total > self.policy.max_rows:
                raise HTTPException(413)
            # The chart contract uses Unix-second numbers, while the legacy CSV
            # reader intentionally retains timestamp strings for table display.
            return self._json([{**row, "time": int(row["time"])} for row in rows])

    def _event(self, message: dict, symbols: set[str]) -> dict | None:
        """Allow only selected-source market fields, including on unscoped broadcasts."""
        kind = message.get("type")
        if kind not in {"tick", "live_tick", "live_bar", "forming_bar"}:
            return None
        data = message.get("data") if kind == "tick" else message
        if not isinstance(data, dict):
            return None
        source = data.get("source")
        alias = next((a for a, internal in self.policy.sources.items() if source == internal), None)
        if alias is None:
            return None
        if kind == "live_bar":
            bars = data.get("bars")
            if not isinstance(bars, list) or not 1 <= len(bars) <= self.policy.max_subscriptions:
                return None
            if any(not isinstance(bar, dict) or bar.get("symbol") not in symbols for bar in bars):
                return None
            return {"type": kind, "source": alias, "has_bars": True,
                    "bars": [_scalar_fields(bar, ("symbol", "time", "open", "high", "low", "close", "volume")) for bar in bars]}
        if kind == "live_tick":
            sample = data.get("sample")
            if not isinstance(sample, dict) or sample.get("symbol") not in symbols:
                return None
            return {"type": kind, "source": alias, "has_ticks": True,
                    "sample": _scalar_fields(sample, ("symbol", "bid", "ask"))}
        if data.get("symbol") not in symbols:
            return None
        if kind == "tick":
            output = _scalar_fields(data, ("symbol", "bid", "ask", "time_ms", "time"))
            output["source"] = alias
            return {"type": kind, "data": output}
        output = _scalar_fields(data, ("symbol", "bar_open_time", "open", "high", "low", "close", "volume"))
        return {"type": kind, "source": alias, **output}

    async def _websocket(self, scope, receive, send):
        """Filter inbound subscriptions/outbound events with bounded nonblocking sends."""
        headers = dict(scope.get("headers", []))
        origin = headers.get(b"origin", b"").decode("latin-1")
        if self._closing or origin not in self.policy.origins or scope.get("query_string") or self._connections >= self.policy.max_connections:
            await send({"type": "websocket.close", "code": 1008})
            return
        self._connections += 1
        symbols = set()
        queue = asyncio.Queue(maxsize=self.policy.ws_queue_size)
        arrivals = deque()
        closed = False
        closed_signal = asyncio.Event()
        shutdown = asyncio.Event()
        close_code = 1000

        def queue_text(text):
            """Queue only sanitized bounded text without waiting on client I/O."""
            if closed or len(text.encode("utf-8")) > self.policy.max_bytes:
                return
            try:
                queue.put_nowait({"type": "websocket.send", "text": text})
            except asyncio.QueueFull:
                closed_signal.set()

        def subscription_status(status, approved):
            """Report semantic rejection without echoing unselected names or closing."""
            queue_text(json.dumps({"type": "subscription_status", "status": status, "symbols": approved}))

        async def close(code=1008):
            """Close at most once on protocol violations, overload or slow consumers."""
            nonlocal closed, close_code
            if not closed:
                closed = True
                close_code = code
                try:
                    await asyncio.wait_for(send({"type": "websocket.close", "code": code}), timeout=2)
                except Exception:
                    pass
                finally:
                    shutdown.set()

        async def receive_or_disconnect():
            """Wake an idle producer after close even if the transport never disconnects."""
            incoming = asyncio.create_task(receive())
            stopping = asyncio.create_task(shutdown.wait())
            try:
                done, _ = await asyncio.wait({incoming, stopping}, return_when=asyncio.FIRST_COMPLETED)
                if stopping in done:
                    return {"type": "websocket.disconnect", "code": close_code}
                return await incoming
            finally:
                for task in (incoming, stopping):
                    if not task.done():
                        task.cancel()
                await asyncio.gather(incoming, stopping, return_exceptions=True)

        async def inbound():
            """Forward only admitted channels; semantic denials keep the socket usable."""
            while True:
                message = await receive_or_disconnect()
                if closed:
                    return {"type": "websocket.disconnect", "code": close_code}
                if message["type"] != "websocket.receive":
                    return message
                text = message.get("text", "")
                now = asyncio.get_running_loop().time()
                while arrivals and arrivals[0] < now - 60:
                    arrivals.popleft()
                arrivals.append(now)
                try:
                    if not isinstance(text, str) or len(text.encode("utf-8")) > 2048 or len(arrivals) > 60:
                        raise ValueError
                    if text == "ping":
                        return message
                    data = json.loads(text)
                    if not isinstance(data, dict) or set(data) != {"type", "symbols"} or data["type"] not in {"subscribe", "unsubscribe"}:
                        raise ValueError
                    requested = data["symbols"]
                    if not isinstance(requested, list) or any(type(s) is not str for s in requested):
                        raise ValueError
                    if len(requested) > self.policy.max_subscriptions:
                        subscription_status("limit_exceeded", [])
                        continue
                    approved = list(dict.fromkeys(s for s in requested if s in self.policy.symbols))
                    if data["type"] == "subscribe":
                        symbols.clear()
                        symbols.update(approved)
                    else:
                        symbols.difference_update(approved)
                    if any(s not in self.policy.symbols for s in requested):
                        subscription_status("partial" if approved else "unavailable", approved)
                    return {"type": "websocket.receive", "text": json.dumps({"type": data["type"], "symbols": approved})}
                except (ValueError, TypeError, KeyError):
                    await close()
                    return {"type": "websocket.disconnect", "code": 1008}

        async def outbound(message):
            """Queue sanitized events immediately so public clients cannot stall ingestion."""
            if closed:
                return
            if message["type"] != "websocket.send":
                if message["type"] == "websocket.accept":
                    await send({"type": "websocket.accept"})
                elif message["type"] == "websocket.close":
                    await close(1000)
                return
            text = message.get("text", "")
            try:
                if text == "pong":
                    clean = "pong"
                else:
                    event = self._event(json.loads(text), symbols)
                    if event is None:
                        return
                    clean = json.dumps(event, allow_nan=False)
                queue_text(clean)
            except (ValueError, TypeError, AttributeError):
                return

        async def writer():
            """Apply per-client send deadlines and close overloaded clients separately."""
            while not closed:
                try:
                    if closed_signal.is_set():
                        await close(1013)
                        return
                    message = await asyncio.wait_for(queue.get(), timeout=1)
                except asyncio.TimeoutError:
                    continue
                try:
                    await asyncio.wait_for(send(message), timeout=2)
                except Exception:
                    await close(1013)
                    return

        task = asyncio.create_task(writer())
        try:
            await self.websocket_app(scope, inbound, outbound)
        finally:
            self._connections -= 1
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task

    async def __call__(self, scope, receive, send):
        """Normalize one API prefix, reject ambiguous paths, and isolate WS transport."""
        if scope["type"] == "http":
            origin = Request(scope).headers.get("origin")
            original_send = send

            async def send_headers(message):
                """Apply browser headers to successes and sanitized boundary errors alike."""
                if message["type"] == "http.response.start":
                    headers = MutableHeaders(scope=message)
                    headers.add_vary_header("Origin")
                    headers["X-Content-Type-Options"] = "nosniff"
                    headers["Referrer-Policy"] = "no-referrer"
                    if origin in self.policy.origins:
                        headers["Access-Control-Allow-Origin"] = origin
                        headers["Access-Control-Expose-Headers"] = "Retry-After"
                await original_send(message)

            send = send_headers
        path = scope.get("path", "")
        if scope["type"] in {"http", "websocket"}:
            if any(s in path for s in ("\\", "//", "%")) or any(p in {".", ".."} for p in path.split("/")):
                if scope["type"] == "websocket":
                    await send({"type": "websocket.close", "code": 1008})
                else:
                    await _safe_error(404)(scope, receive, send)
                return
            if path.startswith("/api/"):
                path = path[4:]
            scope = dict(scope, path=path, raw_path=path.encode("utf-8"))
        if scope["type"] == "websocket":
            if path != "/ws":
                await send({"type": "websocket.close", "code": 1008})
                return
            await self._websocket(scope, receive, send)
        elif scope["type"] == "http":
            await self._serve_http(scope, receive, send)
        else:
            await self.http(scope, receive, send)
