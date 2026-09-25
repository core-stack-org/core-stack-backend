from drf_yasg.generators import OpenAPISchemaGenerator


PUBLIC_API_TAGS = [
    {
        "name": "Dataset APIs v1",
        "description": "Legacy `/api/v1/` dataset routes. Responses are raw JSON.",
    },
    {
        "name": "Dataset APIs v2",
        "description": "`/api/v2/` dataset routes. Responses use `{status, error_message, data}`.",
    },
    {
        "name": "Waterbody APIs v1",
        "description": "Legacy `/api/v1/` waterbody routes.",
    },
    {
        "name": "Waterbody APIs v2",
        "description": "`/api/v2/` waterbody routes. Responses use `{status, error_message, data}`.",
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
