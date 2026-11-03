from unittest.mock import MagicMock, patch

from django.test import SimpleTestCase

from utilities.geoserver_utils import Geoserver


def _resp(status):
    r = MagicMock()
    r.status_code = status
    r.content = b""
    return r


class PublishStyleTests(SimpleTestCase):
    """A missing style must never be written onto a layer (null defaultStyle)."""

    def setUp(self):
        self.geo = Geoserver(service_url="http://gs/geoserver", username="u", password="p")

    def test_missing_style_is_skipped_without_put(self):
        with patch.object(self.geo, "_requests", return_value=_resp(404)) as req:
            result = self.geo.publish_style("lyr", "default_style", "ws")

        self.assertIsNone(result)
        self.assertNotIn("put", [c.args[0] for c in req.call_args_list])

    def test_existing_global_style_is_applied(self):
        def fake(method, url, **kw):
            if method == "get":
                return _resp(200 if url.endswith("/rest/styles/raster.json") else 404)
            return _resp(200)

        with patch.object(self.geo, "_requests", side_effect=fake) as req:
            result = self.geo.publish_style("lyr", "raster", "ws")

        self.assertEqual(result, 200)
        self.assertIn("put", [c.args[0] for c in req.call_args_list])

    def test_workspace_qualified_style_checked_in_its_workspace(self):
        def fake(method, url, **kw):
            if method == "get":
                ok = url.endswith("/rest/workspaces/tree_ch_raster/styles/tree_ch_style.json")
                return _resp(200 if ok else 404)
            return _resp(200)

        with patch.object(self.geo, "_requests", side_effect=fake):
            self.assertTrue(self.geo.style_exists("tree_ch_raster:tree_ch_style", "x"))
            self.assertFalse(self.geo.style_exists("tree_ch_raster:nope", "x"))

    def test_bare_style_falls_back_to_layer_workspace(self):
        def fake(method, url, **kw):
            ok = url.endswith("/rest/workspaces/ws/styles/local_style.json")
            return _resp(200 if ok else 404)

        with patch.object(self.geo, "_requests", side_effect=fake):
            self.assertTrue(self.geo.style_exists("local_style", "ws"))
            self.assertFalse(self.geo.style_exists("local_style", "other"))
