from django.urls import path
from . import api

urlpatterns = [
    path("create_item/", api.create_item, name="create_item"),
    path("map_users_to_community/", api.map_users_to_community, name="map_users_to_community"),
    path("add_user_to_community/", api.add_user_to_community, name="add_user_to_community"),
    path("attach_media_to_item/", api.attach_media_to_item, name="attach_media_to_item"),
    path("get_communities_by_location/", api.get_communities_by_location, name="get_communities_by_location"),
    path("is_user_in_community/", api.is_user_in_community, name="is_user_in_community"),
]