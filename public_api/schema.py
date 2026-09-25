from drf_yasg.generators import OpenAPISchemaGenerator


PUBLIC_API_TAGS = [
    {
        "name": "Dataset APIs v1",
        "description": (
            "Original `/api/v1/` dataset routes. The body is the raw payload, "
            "not a status envelope. Geometry endpoints return a GeoJSON "
            "FeatureCollection with actual polygon rings. Errors are "
            '`{"error": "..."}`. Use this group if you already consume v1.'
        ),
    },
    {
        "name": "Dataset APIs v2",
        "description": (
            "Same dataset routes under `/api/v2/`, wrapped as "
            "`{status, error_message, data}`. Geometry `data` is a "
            "FeatureCollection with unrounded vertices. Filter tehsil sheets "
            "with `data=all` or sheet names, and active locations with "
            "optional `state`, `district`, and `tehsil`."
        ),
    },
    {
        "name": "Waterbody APIs v1",
        "description": (
            "Original `/api/v1/` waterbody routes. Responses are the raw "
            "merged waterbody payload. Errors use the legacy error object. "
            "Look up by admin location or by waterbody UID."
        ),
    },
    {
        "name": "Waterbody APIs v2",
        "description": (
            "Same waterbody routes under `/api/v2/`, wrapped as "
            "`{status, error_message, data}`. Success puts the merged "
            "dataset in `data` with field units. Errors set `error_message` "
            "and leave `data` empty."
        ),
    },
]

_ALLOWED_TAGS = {tag["name"] for tag in PUBLIC_API_TAGS}


def _operation_tags(operation):
    if operation is None:
        return []
    if isinstance(operation, dict):
        return operation.get("tags") or []
    return getattr(operation, "tags", None) or []


class PublicAPISchemaGenerator(OpenAPISchemaGenerator):
    """Keep ReDoc limited to the four public API groups."""

    def get_schema(self, request=None, public=False):
        schema = super().get_schema(request=request, public=public)
        paths = schema.get("paths") or {}
        filtered = {}
        for path, operations in paths.items():
            if not isinstance(operations, dict):
                continue
            kept = {}
            for method, operation in operations.items():
                if str(method).startswith("x-"):
                    kept[method] = operation
                    continue
                if any(tag in _ALLOWED_TAGS for tag in _operation_tags(operation)):
                    kept[method] = operation
            if any(not str(method).startswith("x-") for method in kept):
                filtered[path] = kept
        schema["paths"] = filtered
        schema["tags"] = list(PUBLIC_API_TAGS)
        return schema
