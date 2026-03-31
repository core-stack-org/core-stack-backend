from django.urls import path

from .views import StatusPageView

urlpatterns = [
    path("", StatusPageView.as_view(), name="status-page"),
]
