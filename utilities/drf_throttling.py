"""
Global API rate limiting for Django REST Framework.

Used via REST_FRAMEWORK DEFAULT_THROTTLE_CLASSES and enforce_throttles() for
views wrapped with api_security_check (which bypass normal DRF dispatch).
"""

from django.conf import settings
from rest_framework import status
from rest_framework.settings import api_settings
from rest_framework.throttling import AnonRateThrottle, UserRateThrottle
from django.utils.module_loading import import_string


class CoreStackAnonRateThrottle(AnonRateThrottle):
    scope = "anon"


class CoreStackUserRateThrottle(UserRateThrottle):
    scope = "user"


def throttling_enabled():
    return bool(getattr(settings, "API_RATE_LIMIT_ENABLED", True))


def get_default_throttles():
    classes = api_settings.DEFAULT_THROTTLE_CLASSES
    if not classes:
        return []
    return [import_string(path)() for path in classes]


def enforce_throttles(request, view=None):
    """
    Run configured DRF throttles. Returns a DRF Response when limited, else None.
    """
    if not throttling_enabled():
        return None

    for throttle in get_default_throttles():
        if throttle.allow_request(request, view):
            continue
        wait = throttle.wait()
        detail = "Request was throttled. Expected available in {} second(s).".format(
            int(wait) if wait is not None else 0
        )
        from rest_framework.response import Response

        return Response(
            {
                "error": "Too many requests",
                "detail": detail,
            },
            status=status.HTTP_429_TOO_MANY_REQUESTS,
        )
    return None
