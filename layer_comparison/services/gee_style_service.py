import logging
from functools import lru_cache
from urllib.error import URLError
from urllib.parse import urlsplit
from urllib.request import Request, urlopen
import xml.etree.ElementTree as ET

logger = logging.getLogger(__name__)

_STYLE_HOST = "raw.githubusercontent.com"
_STYLE_PATH_PREFIX = "/core-stack-org/QGIS-Styles/"
_MAX_STYLE_BYTES = 2 * 1024 * 1024
_SLD = "http://www.opengis.net/sld"
_OGC = "http://www.opengis.net/ogc"

ET.register_namespace("sld", _SLD)
ET.register_namespace("ogc", _OGC)


def _color_to_hex(value):
    value = (value or "").split(",rgb:", 1)[0].strip()
    if value.startswith("#") and len(value) in {4, 7, 9}:
        return value[:7] if len(value) == 9 else value

    channels = value.split(",")
    if len(channels) >= 3:
        try:
            red, green, blue = (int(float(channel)) for channel in channels[:3])
        except ValueError:
            return None
        return f"#{red:02x}{green:02x}{blue:02x}"
    return None


def _read_url(url):
    parsed = urlsplit(url)
    if (
        parsed.scheme != "https"
        or parsed.hostname != _STYLE_HOST
        or not parsed.path.startswith(_STYLE_PATH_PREFIX)
    ):
        raise ValueError("GEE style URL must point to the approved QGIS-Styles repository.")

    request = Request(url, headers={"Accept": "application/xml, text/xml"})
    with urlopen(request, timeout=10) as response:
        content = response.read(_MAX_STYLE_BYTES + 1)
    if len(content) > _MAX_STYLE_BYTES:
        raise ValueError("QGIS style file exceeds the maximum supported size.")
    return content


@lru_cache(maxsize=256)
def get_gee_style(style_url):
    if not style_url:
        return {}

    try:
        root = ET.fromstring(_read_url(style_url))
    except (OSError, URLError, TimeoutError, ValueError, ET.ParseError):
        logger.exception("Could not load mapped QGIS style: %s", style_url)
        return {}

    raster_renderer = root.find(".//rasterrenderer")
    if raster_renderer is not None:
        return _parse_raster_style(raster_renderer)

    vector_renderer = root.find(".//renderer-v2")
    if vector_renderer is not None:
        return _parse_vector_style(vector_renderer)

    return {}


def _parse_raster_style(renderer):
    style = {}
    band = renderer.get("band")
    if band and band.isdigit():
        style["_band"] = int(band)

    entries = []
    for entry in renderer.findall(".//paletteEntry"):
        try:
            value = float(entry.attrib["value"])
        except (KeyError, ValueError):
            continue
        color = _color_to_hex(entry.get("color"))
        if color:
            entries.append((value, color, entry.get("alpha", "255") == "0"))

    if not entries:
        for item in renderer.findall(".//colorrampshader/item"):
            try:
                value = float(item.attrib["value"])
            except (KeyError, ValueError):
                continue
            color = _color_to_hex(item.get("color"))
            if color:
                entries.append((value, color, item.get("alpha", "255") == "0"))

    if entries:
        entries.sort(key=lambda entry: entry[0])
        style.update({
            "min": entries[0][0],
            "max": entries[-1][0],
            "palette": [color for _, color, _ in entries],
            "_class_values": [value for value, _, _ in entries],
        })
        transparent_values = [value for value, _, transparent in entries if transparent]
        if transparent_values:
            style["_transparent_values"] = transparent_values
        labels = [
            entry.get("label", "")
            for entry in renderer.findall(".//paletteEntry")
        ] or [item.get("label", "") for item in renderer.findall(".//colorrampshader/item")]
        if (
            renderer.get("type") == "paletted"
            or (
                labels
                and len(labels) == len(entries)
                and all(label and not label.replace(".", "", 1).isdigit() for label in labels)
                and all(value.is_integer() for value, _, _ in entries)
            )
        ):
            style["_discrete"] = True
        return style

    ramp = renderer.find(".//colorrampshader")
    if ramp is not None:
        try:
            style.update({
                "min": float(ramp.get("minimumValue")),
                "max": float(ramp.get("maximumValue")),
            })
            items = []
            for item in ramp.findall("item"):
                color = _color_to_hex(item.get("color"))
                if color:
                    items.append((float(item.get("value")), color))
            if items:
                items.sort(key=lambda entry: entry[0])
                style["palette"] = [color for _, color in items]
            return style
        except (TypeError, ValueError):
            return style
    return style


def _parse_vector_style(renderer):
    categories = renderer.findall("./categories/category")
    symbols = {
        symbol.get("name"): symbol
        for symbol in renderer.findall("./symbols/symbol")
    }

    parsed_categories = []
    for category in categories:
        symbol = symbols.get(category.get("symbol"))
        if symbol is None:
            continue
        color_option = symbol.find(".//Option[@name='color']")
        if color_option is None:
            color_option = symbol.find(".//Option[@name='line_color']")
        color = _color_to_hex(color_option.get("value") if color_option is not None else None)
        if color:
            parsed_categories.append({"value": category.get("value"), "color": color})

    if parsed_categories:
        return {
            "renderer": "categorized",
            "field": renderer.get("attr"),
            "categories": parsed_categories,
        }

    symbol = renderer.find("./symbols/symbol")
    if symbol is not None:
        color_option = symbol.find(".//Option[@name='color']")
        if color_option is None:
            color_option = symbol.find(".//Option[@name='line_color']")
        color = _color_to_hex(color_option.get("value") if color_option is not None else None)
        if color:
            return {"renderer": "single", "color": color}
    return {}


def get_geoserver_sld(style_url, qualified_layer_name, layer_type):
    style = get_gee_style(style_url)
    return build_geoserver_sld(style, qualified_layer_name, layer_type)


def build_geoserver_sld(style, qualified_layer_name, layer_type):
    if not style or not qualified_layer_name:
        return ""

    descriptor = ET.Element(
        f"{{{_SLD}}}StyledLayerDescriptor",
        version="1.0.0",
    )
    named_layer = ET.SubElement(descriptor, f"{{{_SLD}}}NamedLayer")
    ET.SubElement(named_layer, f"{{{_SLD}}}Name").text = qualified_layer_name
    user_style = ET.SubElement(named_layer, f"{{{_SLD}}}UserStyle")
    ET.SubElement(user_style, f"{{{_SLD}}}Title").text = "Mapped QGIS style"

    if layer_type == "raster":
        _add_raster_sld(user_style, style)
    elif layer_type in {"vector", "point"}:
        _add_vector_sld(user_style, style)
    else:
        return ""

    return ET.tostring(descriptor, encoding="unicode")


def _add_raster_sld(user_style, style):
    feature_type_style = ET.SubElement(user_style, f"{{{_SLD}}}FeatureTypeStyle")
    rule = ET.SubElement(feature_type_style, f"{{{_SLD}}}Rule")
    symbolizer = ET.SubElement(rule, f"{{{_SLD}}}RasterSymbolizer")
    channel_selection = ET.SubElement(symbolizer, f"{{{_SLD}}}ChannelSelection")
    gray_channel = ET.SubElement(channel_selection, f"{{{_SLD}}}GrayChannel")
    ET.SubElement(gray_channel, f"{{{_SLD}}}SourceChannelName").text = str(
        style.get("_band", 1)
    )
    color_map = ET.SubElement(
        symbolizer,
        f"{{{_SLD}}}ColorMap",
        type="values" if style.get("_discrete") else "ramp",
    )
    values = style.get("_class_values") or [style.get("min"), style.get("max")]
    colors = style.get("palette", [])
    transparent = set(style.get("_transparent_values", []))
    for value, color in zip(values, colors):
        if value is None or not color:
            continue
        ET.SubElement(
            color_map,
            f"{{{_SLD}}}ColorMapEntry",
            color=color,
            quantity=str(value),
            opacity="0" if value in transparent else "1",
        )


def _add_vector_sld(user_style, style):
    feature_type_style = ET.SubElement(user_style, f"{{{_SLD}}}FeatureTypeStyle")
    categories = style.get("categories") or []
    if categories:
        for category in categories:
            rule = ET.SubElement(feature_type_style, f"{{{_SLD}}}Rule")
            filter_element = ET.SubElement(rule, f"{{{_OGC}}}Filter")
            equals = ET.SubElement(filter_element, f"{{{_OGC}}}PropertyIsEqualTo")
            ET.SubElement(equals, f"{{{_OGC}}}PropertyName").text = style.get("field", "")
            ET.SubElement(equals, f"{{{_OGC}}}Literal").text = str(category["value"])
            _add_vector_symbolizers(rule, category["color"])
        return

    _add_vector_symbolizers(
        ET.SubElement(feature_type_style, f"{{{_SLD}}}Rule"),
        style.get("color", "#3388ff"),
    )


def _add_vector_symbolizers(rule, color):
    line = ET.SubElement(rule, f"{{{_SLD}}}LineSymbolizer")
    line_stroke = ET.SubElement(line, f"{{{_SLD}}}Stroke")
    ET.SubElement(line_stroke, f"{{{_SLD}}}CssParameter", name="stroke").text = color
    ET.SubElement(line_stroke, f"{{{_SLD}}}CssParameter", name="stroke-width").text = "1"

    polygon = ET.SubElement(rule, f"{{{_SLD}}}PolygonSymbolizer")
    fill = ET.SubElement(polygon, f"{{{_SLD}}}Fill")
    ET.SubElement(fill, f"{{{_SLD}}}CssParameter", name="fill").text = color
    ET.SubElement(fill, f"{{{_SLD}}}CssParameter", name="fill-opacity").text = "0.7"
    stroke = ET.SubElement(polygon, f"{{{_SLD}}}Stroke")
    ET.SubElement(stroke, f"{{{_SLD}}}CssParameter", name="stroke").text = color
    ET.SubElement(stroke, f"{{{_SLD}}}CssParameter", name="stroke-width").text = "1"

    point = ET.SubElement(rule, f"{{{_SLD}}}PointSymbolizer")
    graphic = ET.SubElement(point, f"{{{_SLD}}}Graphic")
    mark = ET.SubElement(graphic, f"{{{_SLD}}}Mark")
    ET.SubElement(mark, f"{{{_SLD}}}WellKnownName").text = "circle"
    point_fill = ET.SubElement(mark, f"{{{_SLD}}}Fill")
    ET.SubElement(point_fill, f"{{{_SLD}}}CssParameter", name="fill").text = color
    ET.SubElement(graphic, f"{{{_SLD}}}Size").text = "6"