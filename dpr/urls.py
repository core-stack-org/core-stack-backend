from django.urls import path

from . import api

urlpatterns = [
    path("generate_dpr/", api.generate_dpr, name="generate_dpr"),
]
