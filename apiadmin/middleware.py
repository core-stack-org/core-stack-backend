from .models import ApiHitLog


class ApiHitLoggerMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        response = self.get_response(request)

        # Log only API endpoints
        if request.path.startswith("/api/"):
            api_key = request.headers.get("X-API-KEY") or self.get_auth_api_key(request)

            ApiHitLog.objects.create(
                path=request.path,
                method=request.method,
                user=request.user if request.user.is_authenticated else None,
                ip_address=self.get_client_ip(request),
                query_params=dict(request.GET),
                body=request.body.decode("utf-8")[:5000],
                api_key=api_key,
            )

        return response

    def get_client_ip(self, request):
        x_forwarded_for = request.META.get("HTTP_X_FORWARDED_FOR")
        if x_forwarded_for:
            return x_forwarded_for.split(",")[0]
        return request.META.get("REMOTE_ADDR")

    def get_auth_api_key(self, request):
        """
        Extract API key from "Authorization: Api-Key <key>"
        """
        auth = request.headers.get("Authorization", "")
        if auth.startswith("Api-Key "):
            return auth.replace("Api-Key ", "").strip()
        return None
