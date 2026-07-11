from unittest.mock import MagicMock, patch

from django.test import TestCase

from moderation.utils.update_csdb import sync_form_type


class SyncFormTypeTest(TestCase):

    def test_returns_false_for_unknown_resource_type(self):
        result = sync_form_type("unknown_type")
        self.assertFalse(result)

    @patch("moderation.utils.update_csdb.get_edited_updated_all_submissions")
    @patch("moderation.utils.update_csdb.get_dynamic_filter_query", return_value="$filter=...")
    def test_returns_true_and_calls_resync_on_success(self, mock_filter, mock_client_cls):
        mock_client = MagicMock()
        mock_client.get_edited_updated_submissions.return_value = []
        mock_client_cls.return_value = mock_client

        with patch("moderation.utils.update_csdb.resync_settlement") as mock_resync:
            result = sync_form_type("settlement")

        self.assertTrue(result)
        mock_resync.assert_called_once_with([])

    @patch("moderation.utils.update_csdb.get_edited_updated_all_submissions")
    @patch("moderation.utils.update_csdb.get_dynamic_filter_query", return_value="$filter=...")
    def test_passes_correct_form_id_for_each_type(self, mock_filter, mock_client_cls):
        mock_client = MagicMock()
        mock_client.get_edited_updated_submissions.return_value = []
        mock_client_cls.return_value = mock_client

        from moderation.utils.update_csdb import _FORM_SYNC_CONFIG
        from moderation.utils.form_mapping import corestack

        for resource_type, (form_key, _) in _FORM_SYNC_CONFIG.items():
            mock_client.get_edited_updated_submissions.reset_mock()
            sync_form_type(resource_type)
            call_kwargs = mock_client.get_edited_updated_submissions.call_args[1]
            self.assertEqual(
                call_kwargs["form_id"],
                corestack[form_key],
                msg=f"Wrong form_id for resource_type='{resource_type}'",
            )

    @patch("moderation.utils.update_csdb.get_edited_updated_all_submissions")
    @patch("moderation.utils.update_csdb.get_dynamic_filter_query", return_value="$filter=...")
    def test_returns_false_and_does_not_raise_on_odk_error(self, mock_filter, mock_client_cls):
        mock_client_cls.side_effect = Exception("ODK connection refused")

        result = sync_form_type("settlement")

        self.assertFalse(result)

    @patch("moderation.utils.update_csdb.get_edited_updated_all_submissions")
    @patch("moderation.utils.update_csdb.get_dynamic_filter_query", return_value="$filter=...")
    def test_does_not_update_sync_metadata(self, mock_filter, mock_client_cls):
        mock_client = MagicMock()
        mock_client.get_edited_updated_submissions.return_value = []
        mock_client_cls.return_value = mock_client

        with patch("moderation.models.SyncMetadata.objects") as mock_meta:
            sync_form_type("settlement")
            mock_meta.update.assert_not_called()
            mock_meta.filter.assert_not_called()
