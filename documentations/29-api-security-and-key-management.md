# API Security and Key Management

The Core Stack Backend implements a comprehensive multi-layered security architecture for API access control, supporting both JWT-based authentication for authenticated sessions and API key-based authentication for external integrations. This system provides flexible security controls while maintaining granular tracking of API usage through centralized logging mechanisms.

## Authentication Architecture

The platform employs a dual authentication strategy allowing different security approaches based on use case requirements. Internal authenticated users utilize JWT tokens for session-based access, while external partners and integrations leverage API keys for stateless authentication. This architecture is implemented through a unified decorator system that standardizes authentication checks across all endpoints while maintaining flexibility in authentication methods.

The authentication flow begins with the `api_security_check` decorator in `utilities/auth_check_decorator.py` which acts as the primary security gatekeeper for all API endpoints. This decorator supports three authentication modes: JWT token validation, API key verification, and auth-free endpoints for public data access.

Sources: [utilities/auth\_check\_decorator.py](utilities/auth_check_decorator.py#L10-L70)

### JWT Token Configuration

JWT authentication uses the `rest_framework_simplejwt` library with production-grade configuration. Access tokens remain valid for 90 days while refresh tokens extend to 120 days, with automatic token rotation enabled to enhance security. The system uses HS256 signing algorithm and validates tokens through the Authorization header using the "Bearer" schema.

The JWT configuration in `nrm_app/settings.py` ensures that user identification maps to the custom User model's ID field through the `user_id` claim, enabling seamless integration with the custom user management system.

Sources: [nrm\_app/settings.py](nrm_app/settings.py#L168-L190)

## API Key Management System

API keys provide the primary authentication mechanism for external API consumers and are managed through the `UserAPIKey` model which extends Django REST Framework API Key's abstract base class. Each API key is associated with a specific user, includes metadata for tracking and management, and supports expiration policies for enhanced security control.

The API key model includes essential fields for lifecycle management: a descriptive name for identification, an encrypted `api_key` field storing the actual credential, `is_active` status flag enabling immediate revocation without deletion, and timestamp fields for creation, expiration, and last-used tracking. The `is_expired` property automatically evaluates expiration status based on the `expires_at` field.

Sources: [geoadmin/models.py](geoadmin/models.py#L73-L96)

### API Key Generation

API key creation is centralized through the `generate_api_key` endpoint in `geoadmin/api.py`. This single endpoint handles both key generation and deactivation operations, providing a unified interface for key lifecycle management. The generation process leverages Django REST Framework API Key's secure key generation mechanism which creates cryptographically strong random keys.

When generating a new API key, clients can specify an optional name for the key and an expiration period in days, defaulting to 3000 days for long-term integrations. The response returns the complete key along with metadata including creation timestamp and expiration date. This single-use response is critical—the full key is only returned at creation time and cannot be retrieved later.

Sources: [geoadmin/api.py](geoadmin/api.py#L185-L230)

### API Key Validation and Usage

API key validation occurs automatically through the `api_security_check` decorator. The validation process extracts the API key from the `X-API-Key` HTTP header and verifies it against the `UserAPIKey` model. The system checks both active status and expiration before granting access, with successful validations automatically updating the `last_used_at` timestamp for usage tracking.

For public API endpoints like those in `public_api/api.py`, the decorator pattern ensures consistent authentication enforcement. Endpoints such as `get_admin_details_by_lat_lon` and `get_mws_by_lat_lon` all utilize `@api_security_check(auth_type="API_key")` to require valid API keys before processing requests.

Sources: [utilities/auth\_check\_decorator.py](utilities/auth_check_decorator.py#L140-L160), [public\_api/api.py](public_api/api.py#L30-L34)

## API Monitoring and Logging

Comprehensive API usage tracking is implemented through the `ApiHitLoggerMiddleware` class in `apiadmin/middleware.py`. This middleware automatically logs all requests to `/api/` endpoints, capturing essential information for security monitoring, usage analytics, and debugging purposes.

The logging system captures the request path and HTTP method, authenticated user information when available, client IP address with proper proxy handling, query parameters and request body content, and the API key used for authentication. The `ApiHitLog` model in `apiadmin/models.py` stores this information with timestamps for historical analysis.

Request body content is truncated to 5000 characters in logs to prevent excessive storage usage while preserving enough context for debugging common issues. The middleware properly handles both `X-API-Key` header and `Authorization: Api-Key ` formats for maximum compatibility.

Sources: [apiadmin/middleware.py](apiadmin/middleware.py#L1-L41), [apiadmin/models.py](apiadmin/models.py#L1-L23)

## Security Implementation Patterns

### Endpoint Protection Pattern

The standard pattern for securing API endpoints involves applying the `api_security_check` decorator with the appropriate authentication type parameter. This pattern provides consistent error responses and automatically handles request parsing, authentication validation, and response formatting.

```python
@swagger_auto_schema(**admin_by_latlon_schema)
@api_security_check(auth_type="API_key")
def get_admin_details_by_lat_lon(request):
```

For Swagger/OpenAPI documentation integration, the decorator works seamlessly with `drf_yasg` decorators, ensuring that authentication requirements are properly documented in the API specification.

Sources: [public_api/api.py](.../public_api/api.py#L28-L36)

### Authentication-Free Endpoints

Certain endpoints providing public data without access control use the `Auth_free` authentication type. This pattern allows state or district information retrieval without credentials, as implemented in `geoadmin/api.py` for geographic data endpoints.

```python
@api_security_check(auth_type="Auth_free")
def get_states(request):
    # Public endpoint implementation
```

For complete authentication bypass in special cases, the `@auth_free` decorator from `utilities/auth_utils.py` removes all authentication and permission requirements, though this pattern should be used sparingly with clear justification.

Sources: [geoadmin/api.py](geoadmin/api.py#L23-L36), [utilities/auth\_utils.py](utilities/auth_utils.py#L20-L60)

## CORS and Security Headers

Cross-Origin Resource Sharing is configured in `nrm_app/settings.py` with regex-based origin matching, allowing flexible frontend integration while maintaining security boundaries. The configuration includes support for credentials, custom headers, and specific HTTP methods as required by the frontend application.

CSRF protection is enabled with trusted origins explicitly configured, though many API endpoints use `@csrf_exempt` when implementing token-based authentication to simplify client integration.

Sources: [nrm\_app/settings.py](../nrm_app/settings.py#L131-L154)

## Security Configuration Best Practices

The configuration demonstrates several security best practices including environment-based secret management using `django-environ`, separation of concerns between internal JWT authentication and external API key authentication, comprehensive API usage logging for security monitoring, and support for token revocation and expiration policies.

All sensitive credentials including database passwords, API keys for external services, and authentication tokens are loaded from environment variables rather than hard-coded configuration files.