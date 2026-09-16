from unittest.mock import MagicMock, patch

from botocore import UNSIGNED
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
