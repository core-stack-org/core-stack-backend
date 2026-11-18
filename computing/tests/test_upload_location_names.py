import tempfile
from unittest.mock import MagicMock, patch

from django.core.files.uploadedfile import SimpleUploadedFile
from django.test import SimpleTestCase
from rest_framework.test import APIRequestFactory, force_authenticate

from computing import api
from computing.utils import convert_kml_to_shapefile
from utilities.gee_utils import upload_tif_to_gcs

UNSAFE = ["../etc", "a/b", "a\\b", "/etc/x", "a\x00b"]


def post_form(view, data):
    request = APIRequestFactory().post("/", data, format="multipart")
    force_authenticate(request, user=MagicMock(is_authenticated=True))
    return view(request)


class LocationNameTests(SimpleTestCase):
    def test_real_names_pass(self):
        for name in ["masalia", "west champaran", "kalyan (rural) & co", "st. thomas"]:
            with self.subTest(name=name):
                self.assertIsNone(api._location_path_error(block=name))

    def test_upload_kml_rejects_path_parts(self):
        for name in UNSAFE:
            with self.subTest(name=name), patch("computing.api.kml_to_shp") as convert:
                response = post_form(
                    api.upload_kml,
                    {
                        "state": "s",
                        "district": "d",
                        "block": name,
                        "file": SimpleUploadedFile("a.kml", b"<kml/>"),
                    },
                )
                self.assertEqual(response.status_code, 400)
                convert.assert_not_called()

    def test_clart_upload_rejects_path_parts(self):
        for name in UNSAFE:
            with self.subTest(name=name), patch(
                "computing.api.generate_fes_clart_layer.apply_async"
            ) as task, patch("computing.api.os.makedirs") as makedirs:
                response = post_form(
                    api.fes_clart_upload_layer,
                    {
                        "state": name,
                        "district": "d",
                        "block": "b",
                        "gee_account_id": "1",
                        "clart_file": SimpleUploadedFile("c.tif", b"x"),
                    },
                )
                self.assertEqual(response.status_code, 400)
                makedirs.assert_not_called()
                task.assert_not_called()


class NoShellTests(SimpleTestCase):
    """File names go to GDAL as single arguments, never through a shell."""

    NAME = "d_b; touch pwned"

    def test_ogr2ogr_gets_an_argument_list(self):
        with tempfile.TemporaryDirectory() as out, patch(
            "computing.utils.subprocess.run"
        ) as run:
            convert_kml_to_shapefile("in.kml", out, self.NAME)
        args = run.call_args.args[0]
        self.assertEqual(args[:3], ["ogr2ogr", "-f", "ESRI Shapefile"])
        self.assertTrue(args[3].endswith(f"{self.NAME}.shp"))
        self.assertNotIn("shell", run.call_args.kwargs)

    def test_gdal_translate_gets_an_argument_list(self):
        with patch("utilities.gee_utils.gcs_config"), patch(
            "utilities.gee_utils.time.sleep"
        ), patch("utilities.gee_utils.subprocess.run") as run:
            upload_tif_to_gcs("out.tif", f"/data/{self.NAME}.tif")
        args = run.call_args.args[0]
        self.assertEqual(
            args[:3], ["gdal_translate", f"/data/{self.NAME}.tif", "/data/out_comp.tif"]
        )
        self.assertNotIn("shell", run.call_args.kwargs)
