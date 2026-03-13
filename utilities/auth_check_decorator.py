from functools import wraps
from django.http import JsonResponse
from rest_framework import status
from rest_framework.renderers import JSONRenderer
from rest_framework.response import Response
from django.views.decorators.csrf import csrf_exempt
from django.utils import timezone
import time
import json
from rest_framework_simplejwt.tokens import UntypedToken
from rest_framework_simplejwt.exceptions import InvalidToken, TokenError
import jwt
from django.conf import settings
from django.contrib.auth import get_user_model
from geoadmin.models import UserAPIKey
from rest_framework.decorators import api_view as drf_api_view
from rest_framework.schemas import AutoSchema

User = get_user_model()


def api_security_check(auth_type="JWT", allowed_methods="GET", required_headers=None):
    if isinstance(allowed_methods, str):
        allowed_methods = [allowed_methods]  # Convert string to list

    if required_headers is None:
        required_headers = []

    def decorator(view_func):
        drf_wrapped_func = drf_api_view(allowed_methods)(view_func)

        @wraps(view_func)
        @csrf_exempt
        def wrapper(request, *args, **kwargs):
            try:
                if not hasattr(request, 'query_params'):
                    request.query_params = request.GET

                if not hasattr(request, 'data'):
                    if request.method in ['POST', 'PUT', 'PATCH']:
                        try:
                            content_type = getattr(request, 'content_type', '').lower()

                            if 'application/json' in content_type:
                                import json
                                request.data = json.loads(request.body.decode('utf-8')) if request.body else {}
                            elif 'application/x-www-form-urlencoded' in content_type or 'multipart/form-data' in content_type:
                                request.data = dict(request.POST)
                                for key, value in request.data.items():
                                    if isinstance(value, list) and len(value) == 1:
                                        request.data[key] = value[0]
                            else:
                                try:
                                    request.data = json.loads(request.body.decode('utf-8')) if request.body else {}
                                except:
                                    request.data = dict(request.POST)
                        except (json.JSONDecodeError, UnicodeDecodeError, AttributeError) as parse_error:
                            print(f"Error parsing request data: {parse_error}")
                            request.data = dict(request.POST) if hasattr(request, 'POST') else {}
                    else:
                        request.data = {}

                if request.method not in allowed_methods:
                    return create_drf_response(
                        {"error": f"Method {request.method} not allowed"},
                        status.HTTP_405_METHOD_NOT_ALLOWED
                    )

                auth_result = check_authentication(request, auth_type)
                if not auth_result["valid"]:
                    return create_drf_response(
                        {"error": "Authentication failed", "details": auth_result.get("message", "")},
                        status.HTTP_401_UNAUTHORIZED
                    )

                if "user_info" in auth_result:
                    user_info = auth_result["user_info"]
                    if isinstance(user_info, User):
                        request.user = user_info
                    elif isinstance(user_info, dict) and 'id' in user_info:
                        try:
                            request.user = User.objects.get(id=user_info['id'])
                        except User.DoesNotExist:
                            return create_drf_response(
                                {"error": "User not found"},
                                status.HTTP_401_UNAUTHORIZED
                            )
                    else:
                        request.user = user_info

                if "user" in auth_result:
                    request.user = auth_result["user"]

                missing_headers = validate_required_headers(request, required_headers)
                if missing_headers:
                    return create_drf_response(
                        {"error": "Missing headers", "missing": missing_headers},
                        status.HTTP_400_BAD_REQUEST
                    )

                result = view_func(request, *args, **kwargs)

                if isinstance(result, Response):
                    if not hasattr(result, 'accepted_renderer') or result.accepted_renderer is None:
                        result.accepted_renderer = JSONRenderer()
                        result.accepted_media_type = "application/json"
                        result.renderer_context = {}
                    return result
                elif isinstance(result, dict):
                    return create_drf_response(result)
                else:
                    return result

            except Exception as e:
                print(f"Exception in api_security_check: {e}")
                return create_drf_response(
                    {"error": "Server error", "details": str(e)},
                    status.HTTP_500_INTERNAL_SERVER_ERROR
                )

        wrapper.cls = getattr(drf_wrapped_func, 'cls', None)
        wrapper.initkwargs = getattr(drf_wrapped_func, 'initkwargs', {})

        return wrapper

    return decorator


def create_drf_response(data, status_code=status.HTTP_200_OK):
    """Helper function to create properly configured DRF Response"""
    response = Response(data, status=status_code)
    response.accepted_renderer = JSONRenderer()
    response.accepted_media_type = "application/json"
    response.renderer_context = {}
    return response


def check_authentication(request, auth_type):
    """Check authentication based on type"""
    if auth_type == "Auth_free":
        return {"valid": True}

    elif auth_type == "API_key":
        api_key = request.headers.get("X-API-Key")
        if not api_key:
            return {
                "valid": False,
                "message": "API key required in X-API-Key header",
                "received_headers": dict(request.headers)
            }

        is_valid, user_info = validate_api_key(api_key)
        if is_valid:
            print("validated")
            return {"valid": True, "user_info": user_info}
        else:
            return {
                "valid": False,
                "message": "Invalid API key",
                "api_key_received": api_key[:4] + "..." if api_key else None
            }

    elif auth_type == "JWT":
        auth_header = request.headers.get("Authorization")
        if not auth_header or not auth_header.startswith("Bearer "):
            return {"valid": False, "message": "JWT token required in Authorization header"}

        token = auth_header.split("Bearer ")[1]
        is_valid, user_info = validate_jwt(token)
        if is_valid:
            return {"valid": True, "user_info": user_info}
        else:
            return {"valid": False, "message": "Invalid or expired JWT token"}

    else:
        return {"valid": False, "message": f"Unknown auth type: {auth_type}"}


def validate_required_headers(request, required_headers):
    """Return list of missing headers"""
    print("Validating the header")
    missing = []
    for header in required_headers:
        if header not in request.headers:
            missing.append(header)
    return missing


def validate_api_key(api_key):
    try:
        api_key_obj = UserAPIKey.objects.get_from_key(api_key)

        if not api_key_obj:
            return False, None
        if api_key_obj.is_active and not api_key_obj.is_expired:
            api_key_obj.last_used_at = timezone.now()
            api_key_obj.save()
            return True, api_key_obj.user
        return False, None

    except Exception as e:
        print(f"API Key validation error: {str(e)}")
        return False, None


def validate_jwt(token):
    try:
        payload = jwt.decode(token, settings.SECRET_KEY, algorithms=['HS256'])
        user_id = payload.get('user_id')
        user = User.objects.get(id=user_id)
        return True, user  # Return the actual user object
    except (jwt.ExpiredSignatureError, jwt.InvalidTokenError, User.DoesNotExist):
        return False, None
