"""
Connect these functions to the existing MWS model, PostGIS table, or API.

No MWS model was established from the supplied computing and stats_generator
model references. Do not substitute a tehsil polygon for an MWS polygon.

get_mws_options() must return:
[
    {"id": "<stable MWS id>", "name": "<MWS name>",
     "block": "<optional block>", "district": "<optional district>"}
]

get_mws_geometry(mws_id) must return a GeoJSON Feature/FeatureCollection in
EPSG:4326, with an additional private key used by the view:

{
    "type": "Feature",
    "geometry": {...},
    "properties": {...},
    "_location_context": {
        "state_id": "...",
        "district_id": "...",
        "block_id": "..."
    }
}

The view removes _location_context before returning the geometry to the browser.
Adapt the context keys to the arguments your metadata service actually needs.
"""


def get_mws_options():
    raise NotImplementedError(
        "Connect get_mws_options() to your existing MWS data source."
    )


def get_mws_geometry(mws_id):
    raise NotImplementedError(
        "Connect get_mws_geometry() to your existing MWS geometry source."
    )
