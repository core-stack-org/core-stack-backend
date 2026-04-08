from django.urls import path
from . import api

urlpatterns = [
    path("get_mws_data/", api.get_mws_data_v2, name="get-mws-data-v2"),
]
