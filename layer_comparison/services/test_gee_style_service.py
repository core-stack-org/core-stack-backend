import unittest
import xml.etree.ElementTree as ET

from layer_comparison.services.gee_style_service import (
    build_geoserver_sld,
    _parse_raster_style,
    _parse_vector_style,
)


class GeeStyleParsingTests(unittest.TestCase):
    def test_raster_palette_preserves_class_colors_and_transparency(self):
        renderer = ET.fromstring(
            '<rasterrenderer type="paletted" band="2">'
            '<colorPalette>'
            '<paletteEntry value="0" label="Background" color="#000000" alpha="0" />'
            '<paletteEntry value="1" label="Forest" color="#ff0000" alpha="255" />'
            '<paletteEntry value="2" label="Water" color="#00ff00" alpha="255" />'
            '</colorPalette></rasterrenderer>'
        )

        self.assertEqual(
            _parse_raster_style(renderer),
            {
                "min": 0.0,
                "max": 2.0,
                "palette": ["#000000", "#ff0000", "#00ff00"],
                "_transparent_values": [0.0],
                "_band": 2,
                "_class_values": [0.0, 1.0, 2.0],
                "_discrete": True,
            },
        )

    def test_categorized_vector_style_maps_field_values_to_colors(self):
        renderer = ET.fromstring(
            '<renderer-v2 type="categorizedSymbol" attr="class_name">'
            '<categories>'
            '<category value="Forest" symbol="0" />'
            '<category value="Water" symbol="1" />'
            '</categories><symbols>'
            '<symbol name="0"><layer><Option name="color" value="20,120,40,255" />'
            '</layer></symbol>'
            '<symbol name="1"><layer><Option name="color" value="30,100,220,255" />'
            '</layer></symbol>'
            '</symbols></renderer-v2>'
        )

        self.assertEqual(
            _parse_vector_style(renderer),
            {
                "renderer": "categorized",
                "field": "class_name",
                "categories": [
                    {"value": "Forest", "color": "#147828"},
                    {"value": "Water", "color": "#1e64dc"},
                ],
            },
        )

    def test_raster_sld_contains_palette_and_transparent_class(self):
        sld = build_geoserver_sld(
            {
                "_band": 1,
                "_discrete": True,
                "_class_values": [0.0, 1.0, 2.0],
                "_transparent_values": [0.0],
                "palette": ["#000000", "#ff0000", "#00ff00"],
            },
            "change_detection:change_dumka_masalia_Afforestation",
            "raster",
        )
        root = ET.fromstring(sld)
        entries = root.findall(".//{http://www.opengis.net/sld}ColorMapEntry")

        self.assertEqual(root.findtext(".//{http://www.opengis.net/sld}Name"),
                         "change_detection:change_dumka_masalia_Afforestation")
        self.assertEqual([entry.get("color") for entry in entries], [
            "#000000", "#ff0000", "#00ff00"
        ])
        self.assertEqual(entries[0].get("opacity"), "0")
        self.assertEqual(entries[1].get("opacity"), "1")


if __name__ == "__main__":
    unittest.main()