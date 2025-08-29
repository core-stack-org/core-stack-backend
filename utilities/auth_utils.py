from functools import wraps

from rest_framework.authentication import BaseAuthentication
from rest_framework.decorators import authentication_classes, permission_classes
from rest_framework.permissions import AllowAny


class NoAuthentication(BaseAuthentication):
    """
    Custom authentication class that performs no authentication.
    """

    def authenticate(self, request):
        return None


def auth_free(view_func):
    """
    Decorator to make a view authentication-free.
    This will override the default authentication and permission classes
    for the decorated view function or method.

    Works with both function-based views and class-based views.

    Usage for function-based views:
        @api_view(["GET"])
        @auth_free
        def my_view(request):
            ...

    Usage for class-based views:
        @auth_free
        def list(self, request):
            ...
    """

    view_func = authentication_classes([])(view_func)
    view_func = permission_classes([AllowAny])(view_func)

    @wraps(view_func)
    def wrapper(*args, **kwargs):
        if len(args) > 0 and hasattr(args[0], "authentication_classes"):
            self = args[0]

            original_authentication_classes = self.authentication_classes
            original_permission_classes = self.permission_classes

            self.authentication_classes = []
            self.permission_classes = [AllowAny]

            try:
                return view_func(*args, **kwargs)
            finally:
                self.authentication_classes = original_authentication_classes
                self.permission_classes = original_permission_classes
        else:
            return view_func(*args, **kwargs)

    return wrapper
