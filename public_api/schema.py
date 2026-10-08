from drf_yasg.generators import OpenAPISchemaGenerator


PUBLIC_API_TAGS = [
    {
        "name": "Auth APIs",
        "description": (
            "Create a session, then create an API key. Login returns a JWT. "
            "Generate API Key returns the `X-API-Key` used on Dataset and "
            "Waterbody routes."
        ),
    },
    {
        "name": "Dataset APIs v1",
        "description": (
            "Raw JSON for existing integrations. Geometry routes return a "
            "FeatureCollection. Errors are `{\"error\": \"...\"}`."
        ),
    },
    {
        "name": "Dataset APIs v2",
        "description": (
            "Same resources under `/api/v2/`, wrapped as "
            "`{status, error_message, data}`. Filter tehsil sheets with "
            "`data=` and active locations with `state`, `district`, or `tehsil`."
        ),
    },
    {
        "name": "Waterbody APIs v1",
        "description": (
            "Remotely sensed surface waterbodies for a tehsil, or one record "
            "by UID. Raw JSON."
        ),
    },
    {
        "name": "Waterbody APIs v2",
        "description": (
            "Same waterbody resources under `/api/v2/`, wrapped as "
            "`{status, error_message, data}` with field units."
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
    """Keep ReDoc limited to Auth plus the four public API groups."""

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
