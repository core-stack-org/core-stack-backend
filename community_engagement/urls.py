from django.urls import path
from . import api

urlpatterns = [
    path("create_item/", api.create_item, name="create_item"),
    path("map_users_to_community/", api.map_users_to_community, name="map_users_to_community"),
    path("add_user_to_community/", api.add_user_to_community, name="add_user_to_community"),
    path("attach_media_to_item/", api.attach_media_to_item, name="attach_media_to_item"),
    path("get_communities_by_location/", api.get_communities_by_location, name="get_communities_by_location"),
    path("get_communities_by_lat_lon/", api.get_communities_by_lat_lon, name="get_communities_by_lat_lon"),
    path("is_user_in_community/", api.is_user_in_community, name="is_user_in_community"),
    path("get_community_by_user/", api.get_community_by_user, name="get_community_by_user"),
    path("get_items_by_community/", api.get_items_by_community, name="get_items_by_community"),
    path("get_districts_with_community/", api.get_districts_with_community, name="get_districts_with_community"),
    path("get_blocks_with_community/", api.get_blocks_with_community, name="get_blocks_with_community"),
    path("update_last_accessed_community/", api.update_last_accessed_community, name="update_last_accessed_community"),
]