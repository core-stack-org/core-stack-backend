import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

from django.core.files.uploadedfile import SimpleUploadedFile
from django.test import SimpleTestCase
from rest_framework.test import APIRequestFactory, force_authenticate

from computing import api
from computing.utils import convert_kml_to_shapefile
from utilities.gee_utils import upload_tif_to_gcs

UNSAFE = ["..", "../etc", "a/b", "a\\b", "/etc/x", "a\x00b", "a\nb", "b\n"]


def post_form(view, data):
    request = APIRequestFactory().post("/", data, format="multipart")
    force_authenticate(request, user=MagicMock(is_authenticated=True))
    return view(request)


def files_under(folder):
    return [p for p in Path(folder).rglob("*") if p.is_file()]


class LocationNameTests(SimpleTestCase):
    def test_real_names_pass(self):
        # Real SOI names with the less common characters, lowercased as the views do.
        for name in [
            "masalia",
            "west champaran",
            "west_champaran",
            "lahul & spiti",
            "warangal (urban)",
            "atmakur(m)",
            "k.v.v.b. puram",
            "kotagir1",
        ]:
            with self.subTest(name=name):
                self.assertIsNone(api._location_path_error(block=name))

    def test_path_parts_and_empty_names_fail(self):
        for name in UNSAFE + [""]:
            with self.subTest(name=name):
                self.assertIsNotNone(api._location_path_error(block=name))


class UploadKmlTests(SimpleTestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.kml_dir = tmp.name
        patcher = patch("computing.api.KML_PATH", self.kml_dir + "/")
        patcher.start()
        self.addCleanup(patcher.stop)

    def post(self, block):
        data = {
            "state": "Andhra Pradesh",
            "district": "Anakapalli",
            "block": block,
            "file": SimpleUploadedFile("a.kml", b"<kml/>"),
        }
        return post_form(api.upload_kml, data)

    def test_saves_the_file_and_converts_it(self):
        with patch("computing.api.kml_to_shp") as convert:
            response = self.post("G.Madugula")
        self.assertEqual(response.status_code, 201)
        saved = Path(self.kml_dir) / "a.kml"
        self.assertTrue(saved.is_file())
        convert.assert_called_once_with(
            "andhra pradesh", "anakapalli", "g.madugula", str(saved)
        )

    def test_rejects_path_parts(self):
        for name in UNSAFE:
            with self.subTest(name=name), patch("computing.api.kml_to_shp") as convert:
                self.assertEqual(self.post(name).status_code, 400)
                convert.assert_not_called()
        self.assertEqual(files_under(self.kml_dir), [])


class ClartUploadTests(SimpleTestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.base_dir = tmp.name
        base_dir = patch("computing.api.BASE_DIR", self.base_dir)
        task = patch("computing.api.generate_fes_clart_layer.apply_async")
        base_dir.start()
        self.task = task.start()
        self.addCleanup(base_dir.stop)
        self.addCleanup(task.stop)

    def post(self, state, block="Atmakur(M)"):
        data = {
            "state": state,
            "district": "Nandyal",
            "block": block,
            "gee_account_id": "1",
            "clart_file": SimpleUploadedFile("c.tif", b"x"),
        }
        return post_form(api.fes_clart_upload_layer, data)

    def test_saves_the_file_and_queues_the_task(self):
        response = self.post("Andhra Pradesh")
        self.assertEqual(response.status_code, 200)
        saved = (
            Path(self.base_dir)
            / "data/fes_clart_file/andhra_pradesh/nandyal/nandyal_atmakur(m)_clart_fes.tif"
        )
        self.assertEqual(saved.read_bytes(), b"x")
        self.task.assert_called_once()
        self.assertEqual(
            self.task.call_args.kwargs["args"],
            ["andhra pradesh", "nandyal", "atmakur(m)", str(saved), "1"],
        )

    def test_rejects_path_parts(self):
        for name in UNSAFE:
            with self.subTest(name=name):
                self.assertEqual(self.post(name).status_code, 400)
        self.task.assert_not_called()
        self.assertEqual(files_under(self.base_dir), [])


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
