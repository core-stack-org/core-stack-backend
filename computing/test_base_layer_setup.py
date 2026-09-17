from unittest.mock import MagicMock, patch
import tempfile
from pathlib import Path

from botocore import UNSIGNED
from botocore.exceptions import ClientError
from django.test import SimpleTestCase, override_settings


class S3ClientTests(SimpleTestCase):
    @override_settings(S3_ACCESS_KEY="", S3_SECRET_KEY="", S3_REGION="")
    @patch("boto3.client")
    def test_uses_anonymous_access_without_credentials(self, client):
        from computing.base_layer_setup import _s3_client

        _s3_client()

        kwargs = client.call_args.kwargs
        self.assertEqual(kwargs["config"].signature_version, UNSIGNED)

    @override_settings(
        S3_ACCESS_KEY="key",
        S3_SECRET_KEY="secret",
        S3_REGION="ap-south-1",
    )
    @patch("boto3.client")
    def test_uses_explicit_credentials_when_configured(self, client):
        from computing.base_layer_setup import _s3_client

        _s3_client()

        kwargs = client.call_args.kwargs
        self.assertEqual(kwargs["aws_access_key_id"], "key")
        self.assertEqual(kwargs["aws_secret_access_key"], "secret")
        self.assertNotIn("config", kwargs)

    @override_settings(S3_ACCESS_KEY="", S3_SECRET_KEY="", S3_REGION="")
    @patch("computing.base_layer_setup._s3_client")
    def test_download_uses_get_object_instead_of_head(self, s3_client):
        from computing.base_layer_setup import _download_s3_file

        body = MagicMock()
        body.read.side_effect = [b"abc", b""]
        client = MagicMock()
        client.get_object.return_value = {"Body": body}
        s3_client.return_value = client

        with tempfile.TemporaryDirectory() as tmp:
            destination = Path(tmp) / "layer.tif"
            _download_s3_file(
                "s3://corestack-datasets/base_layers/layer.tif", destination
            )
            client.download_file.assert_not_called()
            client.get_object.assert_called_once_with(
                Bucket="corestack-datasets",
                Key="base_layers/layer.tif",
            )
            self.assertEqual(destination.read_bytes(), b"abc")

    def test_forbidden_errors_are_skippable(self):
        from computing.base_layer_setup import _is_skippable_s3_error

        error = ClientError(
            {
                "Error": {"Code": "403", "Message": "Forbidden"},
                "ResponseMetadata": {"HTTPStatusCode": 403},
            },
            "HeadObject",
        )
        self.assertTrue(_is_skippable_s3_error(error))
