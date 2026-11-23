from unittest.mock import MagicMock, patch

from django.test import SimpleTestCase, override_settings
from rest_framework.test import APIRequestFactory, force_authenticate

from computing import api

# Form and multipart requests deliver every field as text, so "false" must
# still mean False by the time it reaches the task.
CASES = [
    ("false", False),
    ("False", False),
    ("0", False),
    ("true", True),
    ("1", True),
    (None, False),
]

STAC_REQUIRED = {
    "state": "s",
    "district": "d",
    "block": "b",
    "layer_name": "l",
    "layer_type": "vector",
}


def post_form(view, data):
    request = APIRequestFactory().post("/", data)
    force_authenticate(request, user=MagicMock(is_authenticated=True))
    return view(request)


def form(field, value):
    return {} if value is None else {field: value}


class OverwriteParsingTests(SimpleTestCase):
    def test_generate_spei_passes_a_real_boolean(self):
        for value, expected in CASES:
            with self.subTest(value=value), patch(
                "computing.api.generate_spei_pipeline.apply_async"
            ) as task:
                post_form(api.generate_spei, {"aez": "1", **form("overwrite", value)})
                self.assertIs(task.call_args.kwargs["args"][4], expected)

    @override_settings(HEAVY_WORKER_ENABLED=True)
    def test_et_download_passes_a_real_boolean(self):
        for value, expected in CASES:
            with self.subTest(value=value), patch(
                "computing.api.et_download_task.apply_async"
            ) as task:
                post_form(api.et_download, {"state": "s", **form("overwrite", value)})
                self.assertIs(task.call_args.kwargs["kwargs"]["overwrite"], expected)

    def test_generate_stac_collection_passes_real_booleans(self):
        for field in ("overwrite", "overwrite_metadata"):
            for value, expected in CASES:
                with self.subTest(field=field, value=value), patch(
                    "computing.api.generate_stac_collection_task.apply_async"
                ) as task:
                    post_form(
                        api.generate_stac_collection,
                        {**STAC_REQUIRED, **form(field, value)},
                    )
                    self.assertIs(task.call_args.kwargs["kwargs"][field], expected)
