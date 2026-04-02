from rest_framework import status, viewsets, mixins, generics, permissions
from rest_framework.decorators import action, schema
from rest_framework.response import Response
from rest_framework_simplejwt.views import TokenObtainPairView, TokenRefreshView
from rest_framework_simplejwt.tokens import RefreshToken
from rest_framework_simplejwt.authentication import JWTAuthentication
from .models import GEEAccount
from .serializers import GeeAccountSerializers

from organization.models import Organization
import logging

logger = logging.getLogger(__name__)


class GEEAccountView(mixins.ListModelMixin, viewsets.GenericViewSet):
    """API endpoint for GEE accounts."""

    queryset = GEEAccount.objects.filter(is_visible=True)
    serializer_class = GeeAccountSerializers
    permission_classes = [permissions.IsAuthenticated]
    schema = None

    def list(self, request, *args, **kwargs):
        queryset = self.get_queryset()
        serializer = self.get_serializer(queryset, many=True)

        grouped = {}
        for item in serializer.data:
            email = item.get("account_email") or "no_account_email"
            if email not in grouped:
                grouped[email] = []
            grouped[email].append({"id": item["id"], "name": item["name"]})

        return Response(grouped)
