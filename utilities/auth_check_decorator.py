from functools import wraps
from django.http import JsonResponse
from rest_framework import status
from rest_framework.renderers import JSONRenderer
from rest_framework.response import Response
from django.views.decorators.csrf import csrf_exempt
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

def api_security_check(auth_type="JWT", allowed_methods=None, required_headers=None, include_in_schema=True):
    if allowed_methods is None:
        allowed_methods = ["GET"]
    elif isinstance(allowed_methods, str):
        allowed_methods = [allowed_methods]  # Convert string to list
    # else assume it's already a list/tuple
    
    if required_headers is None:
        required_headers = []
    
    def decorator(view_func):
        # Ensure we're passing a list to drf_api_view
        drf_wrapped_func = drf_api_view(allowed_methods)(view_func)
        @wraps(view_func)
        @csrf_exempt
        def wrapper(request, *args, **kwargs):
            try:
                # Ensure request has query_params (DRF compatibility)
                if not hasattr(request, 'query_params'):
                    request.query_params = request.GET
                
                # Rest of your security checks...
                if request.method not in allowed_methods:
                    return create_drf_response(
                        {"error": f"Method {request.method} not allowed"},
                        status.HTTP_405_METHOD_NOT_ALLOWED
                    )

                auth_result = check_authentication(request, auth_type)
                if not auth_result["valid"]:
                    return create_drf_response(
                        {"error": "Authentication failed"},
                        status.HTTP_401_UNAUTHORIZED
                    )

                missing_headers = validate_required_headers(request, required_headers)
                if missing_headers:
                    return create_drf_response(
                        {"error": "Missing headers", "missing": missing_headers},
                        status.HTTP_400_BAD_REQUEST
                    )

                result = view_func(request, *args, **kwargs)
                if isinstance(result, dict):
                    response = Response(result)
                    response.accepted_renderer = JSONRenderer()
                    response.accepted_media_type = "application/json"
                    response.renderer_context = {}
                    return response
                return result
                
            except Exception as e:
                return create_drf_response(
                    {"error": "Server error", "details": str(e)},
                    status.HTTP_500_INTERNAL_SERVER_ERROR
                )
        # Schema visibility control
        if not include_in_schema:
            wrapper.exclude_from_schema = True
        else:
            # Preserve DRF attributes needed for documentation
            wrapper.cls = getattr(drf_wrapped_func, 'cls', None)
            wrapper.initkwargs = getattr(drf_wrapped_func, 'initkwargs', None)
            wrapper.schema = getattr(drf_wrapped_func, 'schema', AutoSchema())

        return wrapper
    return decorator

def create_drf_response(data, status_code=status.HTTP_200_OK):
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
            return {"valid": False, "message": "API key required in X-API-Key header"}
        
        # Your API key validation logic
        is_valid, user_info = validate_api_key(api_key)
        if is_valid:
            return {"valid": True, "user_info": user_info}
        else:
            return {"valid": False, "message": "Invalid API key"}
    
    elif auth_type == "JWT":
        auth_header = request.headers.get("Authorization")
        if not auth_header or not auth_header.startswith("Bearer "):
            return {"valid": False, "message": "JWT token required in Authorization header"}
        
        token = auth_header.split("Bearer ")[1]
        # Your JWT validation logic
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
    """We are using API Key, so validate API key using UserAPIKey model"""
    print("Validating API Key")
    try:
        api_key_obj = UserAPIKey.objects.get_from_key(api_key)
        if api_key_obj and api_key_obj.is_active and not api_key_obj.revoked:
            user_info = {
                "user_id": api_key_obj.user.id,
                "username": api_key_obj.user.username,
                "name": api_key_obj.user.get_full_name() or api_key_obj.user.username,
                "email": api_key_obj.user.email,
                "api_key_id": api_key_obj.id,
                "api_key_name": getattr(api_key_obj, 'name', 'Unnamed Key')
            }
            return True, user_info
        else:
            return False, None
            
    except Exception as e:
        print(str(e))
        return False, None


def validate_jwt(token):
    """We are using JWT, so validate JWT token using rest_framework_simplejwt"""
    print("Validating JWT token")
    try:
        UntypedToken(token)
        decoded_token = jwt.decode(token, settings.SECRET_KEY,  algorithms=["HS256"], options={"verify_signature": True})
        user_id = decoded_token.get('user_id')
        if not user_id:
            return False, None
        
        # Get the user from database
        User = get_user_model()
        user = User.objects.get(id=user_id)
        
        # Return user information
        user_info = {
            "user_id": user.id,
            "username": user.username,
            "name": user.get_full_name() or user.username,
            "email": user.email,
            "is_active": user.is_active,
            "is_staff": user.is_staff,
            "token_type": decoded_token.get('token_type', 'access'),
            "exp": decoded_token.get('exp'),
            "iat": decoded_token.get('iat')
        }
        
        # Check if user is active
        if not user.is_active:
            return False, None
        
        return True, user_info
            
    except Exception as e:
        print("Error in Validating JWT token", str(e))
        return False, None