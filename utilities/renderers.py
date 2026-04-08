from decimal import Decimal

from rest_framework.renderers import JSONRenderer


def round_floats(payload, precision=2):
    if isinstance(payload, dict):
        return {key: round_floats(value, precision) for key, value in payload.items()}
    if isinstance(payload, list):
        return [round_floats(item, precision) for item in payload]
    if isinstance(payload, tuple):
        return tuple(round_floats(item, precision) for item in payload)
    if isinstance(payload, Decimal):
        return round(float(payload), precision)
    if isinstance(payload, float):
        return round(payload, precision)
    return payload


class RoundedJSONRenderer(JSONRenderer):
    """
    Global JSON renderer that trims all float/Decimal values to 2 decimals.
    """

    def render(self, data, accepted_media_type=None, renderer_context=None):
        rounded_data = round_floats(data, precision=2)
        return super().render(
            rounded_data,
            accepted_media_type=accepted_media_type,
            renderer_context=renderer_context,
        )
