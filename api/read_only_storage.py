"""Selected-result adapters for Apollo's Storage/DatasetViewer UI.

The UI's path strings are opaque virtual handles. They are never joined to a
filesystem root or forwarded to operational storage handlers.
"""

import csv
import io
from typing import Callable

from fastapi import HTTPException

from api.result_contracts import TABLE_SCHEMA_VERSION, get_result_schema


class SelectedStorage:
    """Expose only configured selections in the existing browser's wire shapes."""

    def __init__(self, policy, read_rows: Callable):
        """Use the same bounded legacy/indexed row reader as other visitor result routes."""
        self.policy = policy
        self.read_rows = read_rows

    @staticmethod
    def handle(selection) -> str:
        """Return a stable public alias independent of the actual file/cache path."""
        d = selection.description
        if d.date_columns is not None:
            return f"indicators/{d.indicator}/{d.filename}"
        return f"indicators/{selection.description.dataset_id}/data.csv"

    def selection(self, handle: str):
        """Resolve exact handles only; no decoding, path normalization or fallback."""
        for selected in self.policy.results:
            if handle == self.handle(selected):
                return selected
        raise HTTPException(404)

    def tree(self) -> dict:
        """Build a virtual catalog without enumerating or statting storage directories."""
        children = []
        groups = {}
        for selected in self.policy.results:
            description = selected.description
            if description.date_columns is not None:
                folder = f"indicators/{description.indicator}"
                if folder not in groups:
                    groups[folder] = {"id": folder, "path": folder, "name": description.indicator,
                                      "type": "folder", "children": []}
                    children.append(groups[folder])
                handle = self.handle(selected)
                groups[folder]["children"].append({"id": handle, "path": handle,
                    "name": description.filename, "type": "file"})
                continue
            folder = f"indicators/{description.dataset_id}"
            handle = self.handle(selected)
            children.append({
                "id": folder, "path": folder, "name": description.dataset_id,
                "type": "folder", "createdAt": description.generated_at,
                "children": [{"id": handle, "path": handle, "name": "data.csv", "type": "file"}],
            })
        return {"tree": [{"id": "indicators", "path": "indicators", "name": "Indicators",
                          "type": "folder", "children": children}] if children else []}

    def metadata(self, handle: str) -> dict:
        """Return the selected descriptor, never file/folder companion metadata."""
        description = self.selection(handle).description
        result = description.to_dict()
        if description.schema_version == TABLE_SCHEMA_VERSION:
            from api.result_snapshot import page_size
            result["page_size"] = page_size(description, self.policy)
        return result

    def page(self, handle: str, offset: int, limit: int) -> dict:
        """Adapt rows to columnar JSON; full-column snapshots require explicit pages.

        The original viewer requests all rows (limit zero). That means all rows
        within the configured file/scan/response limits, not an unlimited read.
        Missing optional cells use null in this rectangular wire representation;
        stored zero/false/sentinel values remain distinct and unchanged. V2 adds
        revision-pinned pages with explicit next offsets, never a whole-file read.
        """
        selected = self.selection(handle)
        if selected.description.schema_version == TABLE_SCHEMA_VERSION:
            from api.result_snapshot import page_size
            d = selected.description
            if not 0 <= offset <= d.row_count or not 1 <= limit <= page_size(d, self.policy):
                raise HTTPException(422)
            rows, total = self.read_rows(selected, offset, limit)
            return {"__type": "columnar_list", "columns": list(d.columns),
                    "rows": [[row[column] for column in d.columns] for row in rows],
                    "total": total, "offset": offset, "limit": limit,
                    "next_offset": offset + len(rows) if offset + len(rows) < total else None,
                    "revision": d.revision, "paginated": True,
                    "meta": self.metadata(handle)}
        if not 0 <= offset <= self.policy.max_offset or not 0 <= limit <= self.policy.max_rows:
            raise HTTPException(422)
        requested = limit if limit else self.policy.max_rows + 1
        rows, total = self.read_rows(selected, offset, requested)
        if len(rows) > self.policy.max_rows:
            raise HTTPException(413)
        description = selected.description
        schema = get_result_schema(description.result_type, description.schema_version)
        present = {name for row in rows for name in row}
        columns = [name for name in schema.fields if name in present] if rows else list(schema.fields)
        return {"__type": "columnar_list", "columns": columns,
                "rows": [[row.get(name) for name in columns] for row in rows],
                "total": total, "offset": offset, "limit": limit,
                "meta": description.to_dict()}

    def csv(self, page: dict) -> bytes:
        """Serialize only projected cells in memory; no raw file or export job is served."""
        stream = io.StringIO(newline="")
        writer = csv.writer(stream, lineterminator="\r\n")
        writer.writerow(page["columns"])
        for row in page["rows"]:
            writer.writerow([str(value).lower() if type(value) is bool else value for value in row])
        body = stream.getvalue().encode("utf-8")
        if len(body) > self.policy.max_bytes:
            raise HTTPException(413)
        return body
