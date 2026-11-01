"""Original Storage UI compatibility without filesystem discovery or mutations."""

import csv
from dataclasses import replace
import io
from pathlib import Path
import unittest
from unittest.mock import patch
from urllib.parse import urlencode

import httpx

from api.read_only import SelectedResult
from api.read_only_fixture import fixture_application


ORIGIN = "http://127.0.0.1:5181"


class SelectedStorageTests(unittest.IsolatedAsyncioTestCase):
    """Exercise the exact tree/file/meta wire shapes used by the original frontend."""

    async def asyncSetUp(self):
        """Use an in-process visitor app and disposable synthetic selections only."""
        self.fixture = fixture_application((ORIGIN,))
        self.app = self.fixture.__enter__()
        self.client = httpx.AsyncClient(transport=httpx.ASGITransport(app=self.app), base_url=ORIGIN)

    async def asyncTearDown(self):
        """Release only the temporary fixture/client resources owned by this test."""
        await self.client.aclose()
        self.fixture.__exit__(None, None, None)

    def file_url(self, handle="indicators/demo-bb/data.csv", **query):
        """Encode the original UI's opaque path parameter, not a filesystem path."""
        return "/storage/file?" + urlencode({"path": handle, **query})

    async def test_tree_is_a_virtual_selected_catalog_without_directory_scans(self):
        """Unselected files, physical paths and private categories are not discovered."""
        unselected = self.app.policy.result_root / "unselected-fixture.csv"
        unselected.write_text("fixture_private\nnot-public\n", encoding="utf-8")
        with patch.object(Path, "iterdir", side_effect=AssertionError("Directory enumeration forbidden")), \
             patch.object(Path, "rglob", side_effect=AssertionError("Directory enumeration forbidden")):
            response = await self.client.get("/api/storage/tree")
        self.assertEqual(response.status_code, 200)
        tree = response.json()["tree"]
        self.assertEqual([node["name"] for node in tree], ["Indicators"])
        self.assertEqual(len(tree[0]["children"]), len(self.app.policy.results))
        for folder in tree[0]["children"]:
            self.assertEqual(folder["type"], "folder")
            self.assertEqual(folder["id"], folder["path"])
            self.assertEqual(folder["children"], [{
                "id": folder["path"] + "/data.csv", "path": folder["path"] + "/data.csv",
                "name": "data.csv", "type": "file",
            }])
        self.assertNotIn("unselected-fixture", response.text)
        self.assertNotIn(str(self.app.policy.result_root), response.text)

    async def test_all_selected_files_have_the_original_columnar_viewer_shape(self):
        """The unchanged viewer can zip the declared columns and value arrays."""
        for selection in self.app.policy.results:
            d = selection.description
            with self.subTest(kind=d.result_type):
                handle = f"indicators/{d.dataset_id}/data.csv"
                response = await self.client.get(self.file_url(handle, format="json"))
                self.assertEqual(response.status_code, 200)
                payload = response.json()
                self.assertEqual(payload["__type"], "columnar_list")
                self.assertEqual(payload["total"], 1)
                self.assertEqual(payload["offset"], 0)
                self.assertEqual(payload["limit"], 0)
                self.assertTrue(payload["columns"])
                self.assertTrue(all(len(row) == len(payload["columns"]) for row in payload["rows"]))
                # This is the original parseColumnarJSON wire contract, not a new UI.
                row = dict(zip(payload["columns"], payload["rows"][0]))
                expected, _ = self.app._result_rows(selection, 0, 1)
                self.assertEqual(row, expected[0])
                self.assertEqual(payload["meta"], d.to_dict())

    async def test_meta_and_file_views_ignore_companion_metadata_and_unknown_columns(self):
        """The original metadata display receives only the explicit public descriptor."""
        selection = next(r for r in self.app.policy.results if r.description.result_type == "triggers")
        with selection.path.open(newline="", encoding="utf-8") as stream:
            row = next(csv.DictReader(stream))
        row["fixture_private_column"] = "PRIVATE_FIXTURE_ONLY"
        with selection.path.open("w", newline="", encoding="utf-8") as stream:
            writer = csv.DictWriter(stream, fieldnames=list(row))
            writer.writeheader()
            writer.writerow(row)
        for path in (selection.path.with_suffix(".meta.json"), Path(str(selection.path) + ".meta.json"),
                     selection.path.parent / "_meta.json"):
            path.write_text('{"private_fixture_meta":"PRIVATE_FIXTURE_ONLY"}', encoding="utf-8")
        handle = "indicators/demo-triggers/data.csv"
        meta = await self.client.get("/storage/meta?" + urlencode({"path": handle}))
        self.assertEqual(meta.status_code, 200)
        self.assertEqual(meta.json(), selection.description.to_dict())
        for options in ({"format": "json"}, {}):
            response = await self.client.get(self.file_url(handle, **options))
            self.assertEqual(response.status_code, 200)
            self.assertNotIn("PRIVATE_FIXTURE_ONLY", response.text)
            self.assertNotIn("fixture_private_column", response.text)
            self.assertNotIn(str(selection.path), response.text)

    async def test_csv_fallback_is_projected_and_preserves_existing_scalar_format(self):
        """No raw file/download job is needed for the viewer's text/csv fallback."""
        response = await self.client.get(self.file_url("indicators/demo-triggers/data.csv"))
        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.headers["content-type"].startswith("text/csv"))
        row = next(csv.DictReader(io.StringIO(response.text)))
        self.assertEqual(row["p_long"], "0")
        self.assertEqual(row["atr_short"], "")
        self.assertEqual(row["p_short"], "insufficient_bars")
        levels = await self.client.get(self.file_url("indicators/demo-levels/data.csv"))
        self.assertEqual(next(csv.DictReader(io.StringIO(levels.text)))["dashed"], "false")

    async def test_opaque_handles_cannot_select_private_or_arbitrary_paths(self):
        """No path normalization, encoded traversal, companion or root fallback exists."""
        handles = ("../fixture.csv", "indicators/../fixture.csv", "C:\\fixture.csv", "/fixture.csv",
                   "indicators\\demo-bb\\data.csv", "indicators/demo-bb/data.csv.meta.json",
                   "indicators/unknown/data.csv", "indicators/demo-bb//data.csv",
                   "indicators/demo-bb/./data.csv", "indicators/%2e%2e/fixture.csv")
        with patch.object(self.app, "_result_rows", side_effect=AssertionError("Unselected read forbidden")):
            for handle in handles:
                with self.subTest(handle=handle):
                    response = await self.client.get(self.file_url(handle, format="json"))
                    self.assertEqual(response.status_code, 404)
            meta = await self.client.get("/storage/meta?path=indicators/unknown/data.csv")
            self.assertEqual(meta.status_code, 404)

    async def test_unknown_duplicate_and_invalid_options_fail_before_file_work(self):
        """The legacy URL is not permission to choose columns, roots or output modes."""
        with patch.object(self.app, "_result_rows", side_effect=AssertionError("Invalid read forbidden")):
            for query in ({"format": "raw"}, {"format": "json", "limit": -1},
                          {"format": "json", "offset": -1}, {"format": "json", "limit": "invalid"},
                          {"format": "json", "limit": self.app.policy.max_rows + 1},
                          {"format": "json", "columns": "fixture_private"}):
                with self.subTest(query=query):
                    self.assertEqual((await self.client.get(self.file_url(**query))).status_code, 422)
            duplicate = self.file_url(format="json") + "&path=indicators/other/data.csv"
            self.assertEqual((await self.client.get(duplicate)).status_code, 422)
            self.assertEqual((await self.client.get("/storage/tree?root=private")).status_code, 422)
            self.assertEqual((await self.client.get("/storage/meta")).status_code, 422)

    async def test_unpaged_results_fail_instead_of_silently_truncating(self):
        """Limit zero means all bounded rows; explicit pages retain total/offset."""
        selection = self.app.policy.results[0]
        selection.path.write_text("time,pct\n1704067200,0.25\n1704067260,\n1704067320,0\n", encoding="utf-8")
        self.app.policy = replace(self.app.policy, max_rows=2)
        self.assertEqual((await self.client.get(self.file_url(format="json"))).status_code, 413)
        self.assertEqual((await self.client.get(self.file_url())).status_code, 413)
        response = await self.client.get(self.file_url(format="json", offset=1, limit=1))
        payload = response.json()
        self.assertEqual(payload["total"], 3)
        self.assertEqual(payload["offset"], 1)
        self.assertEqual(payload["rows"], [["1704067260", None]])
        remainder = (await self.client.get(self.file_url(format="json", offset=1))).json()
        self.assertEqual(remainder["rows"], [["1704067260", None], ["1704067320", 0]])

    async def test_selected_handles_still_enforce_physical_containment_and_byte_limits(self):
        """A selected alias does not bypass the shared file/scan/response budgets."""
        original = self.app.policy.results[0]
        outside = Path(self.app.policy.result_root.parent) / "outside-fixture.csv"
        self.app.policy = replace(self.app.policy, results=(SelectedResult(original.description, outside),))
        self.assertEqual((await self.client.get(self.file_url(format="json"))).status_code, 404)
        self.app.policy = replace(self.app.policy, results=(original,), max_file_bytes=1)
        self.assertEqual((await self.client.get(self.file_url(format="json"))).status_code, 413)
        self.app.policy = replace(self.app.policy, max_file_bytes=100000, max_bytes=64)
        self.assertEqual((await self.client.get(self.file_url(format="json"))).status_code, 413)

    async def test_empty_declared_schema_succeeds_but_unknown_empty_layout_fails(self):
        """Empty results are valid only when their required legacy columns are known."""
        selection = self.app.policy.results[0]
        selection.path.write_text("time,pct\n", encoding="utf-8")
        payload = (await self.client.get(self.file_url(format="json"))).json()
        self.assertEqual(payload["columns"], ["time", "pct"])
        self.assertEqual(payload["rows"], [])
        self.assertEqual(payload["total"], 0)
        selection.path.write_text("fixture_private\n", encoding="utf-8")
        response = await self.client.get(self.file_url(format="json"))
        self.assertEqual(response.status_code, 422)
        self.assertNotIn("fixture_private", response.text)

    async def test_all_storage_mutations_remain_denied_and_fixture_content_is_unchanged(self):
        """Compatibility reads do not revive uploads, rename/delete or export jobs."""
        before = {r.path: r.path.read_bytes() for r in self.app.policy.results}
        for method, path in (("POST", "/storage/rename"), ("DELETE", "/storage/delete"),
                             ("POST", "/storage/upload"), ("POST", "/storage/export")):
            response = await self.client.request(method, path, json={"path": "indicators/demo-bb/data.csv"},
                                                 headers={"X-Role": "operator"})
            self.assertEqual(response.status_code, 403)
        self.assertGreaterEqual((await self.client.get("/storage/export/status?job_id=fixture")).status_code, 400)
        self.assertGreaterEqual((await self.client.get("/storage/export/download?job_id=fixture")).status_code, 400)
        self.assertEqual(before, {r.path: r.path.read_bytes() for r in self.app.policy.results})


if __name__ == "__main__":
    unittest.main()
