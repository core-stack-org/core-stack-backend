import math
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
        try:
            f = float(payload)
        except Exception:
            return None
        if not math.isfinite(f):
            return None
        return round(f, precision)
    if isinstance(payload, float):
        if not math.isfinite(payload):
            return None
        return round(payload, precision)
    try:
        import numpy as np

        if isinstance(payload, np.generic):
            if np.issubdtype(type(payload), np.floating):
                val = float(payload)
                if not math.isfinite(val):
                    return None
                return round(val, precision)
            if np.issubdtype(type(payload), np.integer):
                return int(payload)
            if np.issubdtype(type(payload), np.bool_):
                return bool(payload)
    except ImportError:
        pass
    try:
        import pandas as pd

        if payload is pd.NA:
            return None
    except ImportError:
        pass
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
