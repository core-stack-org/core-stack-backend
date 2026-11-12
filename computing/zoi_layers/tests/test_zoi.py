from unittest.mock import MagicMock, patch

from django.test import SimpleTestCase

from computing.zoi_layers.zoi1 import create_ring
from computing.zoi_layers.zoi2 import generate_zoi_ci


class ZoiPipelineTests(SimpleTestCase):
    @patch("computing.zoi_layers.zoi1.calculate_zoi_area", return_value=123)
    @patch("computing.zoi_layers.zoi1.ee")
    def test_create_ring_preserves_local_waterbody_id(self, ee, calculate_area):
        feature = MagicMock()
        feature.get.side_effect = {
            "UID": None,
            "wb_id": "waterbody-1",
            "zoi_wb": 500,
        }.get
        ee.Number.side_effect = lambda value: value
        ee.Algorithms.IsEqual.side_effect = lambda left, right: left == right
        ee.Algorithms.If.side_effect = (
            lambda condition, true_value, false_value: (
                true_value if condition else false_value
            )
        )

        create_ring(feature)

        properties = ee.Feature.return_value.set.call_args.args[0]
        self.assertEqual(properties["UID"], "waterbody-1")
        self.assertEqual(properties["wb_id"], "waterbody-1")

    @patch(
        "computing.cropping_intensity.cropping_intensity."
        "generate_cropping_intensity"
    )
    @patch("computing.zoi_layers.zoi2.sync_asset_to_db_and_geoserver")
    @patch("computing.zoi_layers.zoi2.delete_asset_on_GEE")
    @patch(
        "computing.zoi_layers.zoi2.get_gee_dir_path",
        return_value="projects/example/state/district/block/",
    )
    def test_rerun_deletes_final_zoi_cropping_intensity_asset(
        self, get_dir, delete_asset, sync_asset, generate_ci
    ):
        generate_zoi_ci(
            state="Rajasthan",
            district="Raj Samand",
            block="Bhim",
            gee_account_id="22",
            start_date="2017-07-01",
            end_date="2018-06-30",
            start_year=2017,
            end_year=2017,
        )

        output_asset_id = (
            "projects/example/state/district/block/"
            "cropping_intensity_zoi_raj_samand_bhim"
        )
        delete_asset.assert_called_once_with(output_asset_id)
        self.assertEqual(
            generate_ci.call_args.kwargs["zoi_ci_asset"],
            output_asset_id,
        )
        self.assertEqual(generate_ci.call_args.kwargs["state"], "Rajasthan")
        self.assertEqual(generate_ci.call_args.kwargs["district"], "Raj Samand")
        self.assertEqual(generate_ci.call_args.kwargs["block"], "Bhim")
