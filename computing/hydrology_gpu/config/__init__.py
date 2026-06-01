from pathlib import Path

try:
    import tomllib
except ModuleNotFoundError:
    tomllib = None


def _loads_config(text):
    if tomllib is not None:
        return tomllib.loads(text)

    values = {}
    for raw_line in text.splitlines():
        line = raw_line.split("#", 1)[0].strip()
        if not line:
            continue
        key, raw_value = line.split("=", 1)
        key = key.strip()
        raw_value = raw_value.strip()
        if raw_value.startswith(("'", '"')) and raw_value.endswith(("'", '"')):
            values[key] = raw_value[1:-1]
        else:
            try:
                values[key] = int(raw_value)
            except ValueError:
                values[key] = raw_value
    return values


_config_path = Path(__file__).with_name("config.toml")
_values = _loads_config(_config_path.read_text())

globals().update(_values)

# The current runner uses the same vector file as both the region boundary and
# the microwatershed feature collection unless the CLI overrides it.
MICROWATERSHEDS_PATH = BOUNDARY_GEOJSON_PATH
