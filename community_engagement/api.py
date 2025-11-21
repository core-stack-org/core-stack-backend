import ast
import boto3
import json
import pandas as pd
import traceback
import uuid
from collections import defaultdict

from django.http import HttpRequest
from rest_framework import status
from rest_framework.decorators import api_view, parser_classes, schema
from rest_framework.parsers import MultiPartParser, FormParser
from rest_framework.response import Response

from drf_yasg import openapi
from drf_yasg.utils import swagger_auto_schema

from .models import (
    Community,
    Item,
    Community_user_mapping,
    Media,
    Media_type,
    Location,
    Item_type,
    Item_state,
    ITEM_TYPE_STATE_MAP,
    Item_category,
)
from .utils import (
    get_media_type,
    get_community_summary_data,
    get_communities,
    generate_item_title,
)
from geoadmin.models import State, District, Block
from projects.models import Project, AppType
from users.models import User
from bot_interface.models import Bot
from public_api.views import get_location_info_by_lat_lon
from utilities.auth_utils import auth_free
from nrm_app.settings import S3_BUCKET, S3_REGION
from utilities.auth_check_decorator import api_security_check
from .utils import create_community_for_project


# Common parameters that can be reused across endpoints
item_id = openapi.Parameter(
    "item_id",
    openapi.IN_FORM,
    description="Unique item ID.",
    type=openapi.TYPE_STRING,
    required=False,
)
title = openapi.Parameter(
    "title",
    openapi.IN_FORM,
    description="Title of the item.",
    type=openapi.TYPE_STRING,
    required=False,
)
transcript = openapi.Parameter(
    "transcript",
    openapi.IN_FORM,
    description="Transcript text.",
    type=openapi.TYPE_STRING,
    required=False,
)
category_id = openapi.Parameter(
    "category_id",
    openapi.IN_FORM,
    description="Category identifier.",
    type=openapi.TYPE_INTEGER,
    required=True,
)
rating = openapi.Parameter(
    "rating",
    openapi.IN_FORM,
    description="Rating value.",
    type=openapi.TYPE_INTEGER,
    required=False,
)
item_type = openapi.Parameter(
    "item_type",
    openapi.IN_FORM,
    description="Type of the item.",
    type=openapi.TYPE_STRING,
    required=True,
)
coordinates = openapi.Parameter(
    "coordinates",
    openapi.IN_FORM,
    description='JSON string of coordinates, e.g., \'{"lat": 28.1, "lon": 77.5}\'',
    type=openapi.TYPE_STRING,
    required=True,
)
community_id = openapi.Parameter(
    "community_id",
    openapi.IN_FORM,
    description="Community identifier.",
    type=openapi.TYPE_INTEGER,
    required=True,
)
number = openapi.Parameter(
    "number",
    openapi.IN_FORM,
    description="User's contact number.",
    type=openapi.TYPE_STRING,
    required=True,
)
source = openapi.Parameter(
    "source",
    openapi.IN_FORM,
    description="Source of the data (e.g., 'BOT').",
    type=openapi.TYPE_STRING,
    required=True,
)
bot_id = openapi.Parameter(
    "bot_id",
    openapi.IN_FORM,
    description="Bot ID (if source is 'BOT').",
    type=openapi.TYPE_INTEGER,
    required=False,
)
misc = openapi.Parameter(
    "misc",
    openapi.IN_FORM,
    description="Miscellaneous JSON data string.",
    type=openapi.TYPE_STRING,
    required=False,
)
image_files = openapi.Parameter(
    "image_files",
    openapi.IN_FORM,
    description="Image file(s) to upload.",
    type=openapi.TYPE_FILE,
    required=False,
)
audio_files = openapi.Parameter(
    "audio_files",
    openapi.IN_FORM,
    description="Audio file(s) to upload.",
    type=openapi.TYPE_FILE,
    required=False,
)
state = openapi.Parameter(
    "state", openapi.IN_FORM, type=openapi.TYPE_STRING, description="State of the item"
)

response_param = openapi.Parameter(
    "X-API-Key",
    openapi.IN_HEADER,
    description="API Key in format: <your-api-key>",
    type=openapi.TYPE_STRING,
    required=True,
)


def attach_media_files(files, item, user, source, bot=None):
    s3_client = boto3.client("s3")

    bot_instance = None
    if bot:
        bot_instance = Bot.objects.filter(id=bot).first()

    media_dict = defaultdict(list)
    for key in files:
        for file in files.getlist(key):
            media_dict[key].append(file)

    for media_key, media_files in media_dict.items():
        media_type = get_media_type(media_key)
        if not media_type:
            print(f"Skipping unknown media key: {media_key}")
            continue

        s3_folder = f"{media_type.lower()}s"
        for media_file in media_files:
            extension = media_file.name.split(".")[-1]
            s3_key = f"{s3_folder}/{uuid.uuid4()}.{extension}"

            try:
                s3_client.upload_fileobj(media_file, S3_BUCKET, s3_key)
            except Exception as e:
                print(f"Failed to upload {media_file.name} to S3:", e)
                continue

            media_path = f"https://{S3_BUCKET}.s3.{S3_REGION}.amazonaws.com/{s3_key}"
            media_obj = Media.objects.create(
                user=user,
                media_type=media_type,
                media_path=media_path,
                source=source,
                bot=bot_instance,
            )
            item.media.add(media_obj)


def handle_media_upload(request, item, user, source, bot_id=None):
    if not request.FILES:
        return

    if not source:
        raise ValueError("Missing source for media upload.")

    attach_media_files(request.FILES, item, user, source, bot_id)


####### Add and Update items ###################
@swagger_auto_schema(
    method="post",
    operation_id="upsert_item",
    operation_summary="Create or Update an Item",
    operation_description="""
    Create a new item or update an existing item with media upload support.",
    **Response dataset details:**
    ```
    {
        "success": true,
        "message": "Item created",
        "item_id": 12
    }
    ```
    """,
    manual_parameters=[
        item_id,
        title,
        transcript,
        category_id,
        rating,
        item_type,
        coordinates,
        community_id,
        number,
        source,
        bot_id,
        misc,
        state,
        image_files,
        audio_files,
    ],
    responses={
        201: openapi.Response(
            description="Created - Item created successfully",
            examples={
                "application/json": {
                    "success": True,
                    "message": "Item created",
                    "item_id": 12,
                }
            },
        ),
        200: openapi.Response(
            description="Success - Item updated successfully",
            examples={
                "application/json": {
                    "success": True,
                    "message": "Item updated",
                    "item_id": 12,
                }
            },
        ),
        400: openapi.Response(
            description="Bad Request - Missing or invalid parameters",
            examples={
                "application/json": {
                    "success": False,
                    "message": "Missing required fields for item creation.",
                }
            },
        ),
        404: openapi.Response(
            description="Not Found - Item not found",
            examples={
                "application/json": {"success": False, "message": "Item not found"}
            },
        ),
        500: openapi.Response(
            description="Internal Server Error",
            examples={
                "application/json": {
                    "success": False,
                    "message": "Internal Server Error",
                }
            },
        ),
    },
    tags=["Community Engagement APIs"],
)
@api_security_check(allowed_methods="POST", auth_type="Auth_free")
@parser_classes([MultiPartParser, FormParser])
def upsert_item(request):
    try:
        item_id = request.data.get("item_id")
        title = request.data.get("title", "")
        transcript = request.data.get("transcript", "")
        category_id = request.data.get("category_id")
        rating = request.data.get("rating")
        rating = int(rating) if rating is not None else 0
        item_type = request.data.get("item_type")
        coordinates = request.data.get("coordinates")
        community_id = request.data.get("community_id")
        number = request.data.get("number")
        source = request.data.get("source")
        bot_id = request.data.get("bot_id")
        misc_data = request.data.get("misc", {})

        if source == "BOT" and not bot_id:
            return Response(
                {"success": False, "message": "bot_id is required when source is BOT"},
                status=status.HTTP_400_BAD_REQUEST,
            )

        user = User.objects.filter(contact_number=number).first()
        if not user:
            return Response(
                {"success": False, "message": f"User with number {number} not found."},
                status=status.HTTP_400_BAD_REQUEST,
            )

        if category_id and not Item_category.objects.filter(id=category_id).exists():
            return Response(
                {"success": False, "message": "Invalid category_id"},
                status=status.HTTP_400_BAD_REQUEST,
            )

        if item_id:
            try:
                item = Item.objects.get(id=item_id)
            except Item.DoesNotExist:
                return Response(
                    {"success": False, "message": "Item not found"},
                    status=status.HTTP_404_NOT_FOUND,
                )

            if title:
                item.title = title
            if transcript:
                item.transcript = transcript
            if category_id:
                item.category_id = category_id
            if rating is not None:
                item.rating = rating
            if item_type:
                item.item_type = item_type
            if coordinates:
                item.coordinates = coordinates
            if misc_data:
                current_misc = item.misc if isinstance(item.misc, dict) else {}
                item.misc = {**current_misc, **misc_data}
            if request.data.get("state"):
                item.state = request.data["state"]

            item.save()

            try:
                handle_media_upload(request, item, user, source, bot_id)
            except ValueError as e:
                return Response(
                    {"success": False, "message": str(e)},
                    status=status.HTTP_400_BAD_REQUEST,
                )

            return Response(
                {"success": True, "message": "Item updated", "item_id": item.id},
                status=status.HTTP_200_OK,
            )

        mandatory_fields = [item_type, coordinates, number, community_id]
        if not all(mandatory_fields):
            return Response(
                {
                    "success": False,
                    "message": "Missing required fields for item creation.",
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        if not Community.objects.filter(id=community_id).exists():
            return Response(
                {"success": False, "message": "Invalid community_id"},
                status=status.HTTP_400_BAD_REQUEST,
            )

        if not title.strip():
            title = generate_item_title(item_type)

        item = Item.objects.create(
            title=title,
            transcript=transcript,
            category_id=category_id,
            rating=rating,
            item_type=item_type,
            coordinates=coordinates,
            state=Item_state.UNMODERATED,
            user=user,
            community_id=community_id,
            misc=misc_data if isinstance(misc_data, dict) else {},
        )

        try:
            handle_media_upload(request, item, user, source, bot_id)
        except ValueError as e:
            return Response(
                {"success": False, "message": str(e)},
                status=status.HTTP_400_BAD_REQUEST,
            )

        return Response(
            {"success": True, "message": "Item created", "item_id": item.id},
            status=status.HTTP_201_CREATED,
        )

    except Exception:
        print("Exception in upsert_item API:", traceback.format_exc())
        return Response(
            {"success": False, "message": "Internal Server Error"},
            status=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )


######## Attach media to items #############


@swagger_auto_schema(
    method="post",
    operation_id="attach_media_to_item",
    operation_summary="Attach Media to an Item",
    operation_description="""
    Attach media files (images/audios) to an existing item.
    **Response dataset details:**
    ```
    {
    "success": true,
    "message": "Media attached successfully"
    }
    ```
    """,
    manual_parameters=[item_id, number, source, bot_id],
    responses={
        200: openapi.Response(
            description="Success - Media attached successfully",
            examples={
                "application/json": {
                    "success": True,
                    "message": "Media attached successfully",
                }
            },
        ),
        400: openapi.Response(
            description="Bad Request - Missing number OR bot_id is required when source is BOT",
            examples={
                "application/json": {
                    "success": False,
                    "message": "Missing number OR bot_id is required when source is BOT.",
                }
            },
        ),
        404: openapi.Response(
            description="Not Found - Item not found",
            examples={
                "application/json": {"success": False, "message": "Item not found"}
            },
        ),
        500: openapi.Response(
            description="Internal Server Error",
            examples={
                "application/json": {
                    "success": False,
                    "message": "Internal server error",
                }
            },
        ),
    },
    tags=["Community Engagement APIs"],
)
@api_view(["POST"])
@auth_free
@parser_classes([MultiPartParser, FormParser])
def attach_media_to_item(request):
    try:
        item_id = request.data.get("item_id")
        number = request.data.get("number")
        source = request.data.get("source")
        bot_id = request.data.get("bot_id")

        if not item_id or not number:
            return Response(
                {"success": False, "message": "Missing item_id or number"},
                status=status.HTTP_400_BAD_REQUEST,
            )

        if source == "BOT" and not bot_id:
            return Response(
                {"success": False, "message": "bot_id is required when source is BOT"},
                status=status.HTTP_400_BAD_REQUEST,
            )

        user = User.objects.filter(contact_number=number).first()
        if not user:
            return Response(
                {"success": False, "message": "User with number not found."},
                status=status.HTTP_400_BAD_REQUEST,
            )

        try:
            item = Item.objects.get(id=item_id)
        except Item.DoesNotExist:
            return Response(
                {"success": False, "message": "Item not found"},
                status=status.HTTP_404_NOT_FOUND,
            )

        try:
            handle_media_upload(request, item, user, source, bot_id)
        except ValueError as e:
            return Response(
                {"success": False, "message": str(e)},
                status=status.HTTP_400_BAD_REQUEST,
            )

        return Response(
            {"success": True, "message": "Media attached successfully"},
            status=status.HTTP_200_OK,
        )

    except Exception:
        print("Exception in attach_media_to_item:", traceback.format_exc())
        return Response(
            {"success": False, "message": "Internal server error"},
            status=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )


########## Get Community Details   #############


@swagger_auto_schema(
    method="get",
    operation_id="get_community_details",
    operation_summary="Get Community Details",
    operation_description="""
    Retrieve detailed information about a specific community.
    **Response dataset details:**
    ```
    {
    "success": true,
    "data": {
        "id": 1,
        "name": "Community Name",
        "description": "Community description",
        "organization": "Organization Name",
        "member_count": 150,
        "location": "Location details",
        "created_at": "2023-01-01T00:00:00Z"
    }
    }
    ```
    """,
    manual_parameters=[
        openapi.Parameter(
            "community_id",
            openapi.IN_QUERY,
            description="ID of the community",
            type=openapi.TYPE_INTEGER,
            required=True,
        )
    ],
    responses={
        200: openapi.Response(
            description="Success - Returns community details",
            examples={
                "application/json": {
                    "success": True,
                    "data": {
                        "id": 1,
                        "name": "Community Name",
                        "description": "Community description",
                        "organization": "Organization Name",
                        "member_count": 150,
                        "location": "Location details",
                        "created_at": "2023-01-01T00:00:00Z",
                    },
                }
            },
        ),
        400: openapi.Response(
            description="Bad Request - The community_id parameter is required.",
            examples={
                "application/json": {
                    "success": False,
                    "message": "The 'community_id' parameter is required.",
                }
            },
        ),
        500: openapi.Response(
            description="Internal Server Error",
            examples={
                "application/json": {
                    "success": False,
                    "message": "An internal server error occurred.",
                }
            },
        ),
    },
    tags=["Community Engagement APIs"],
)
@api_view(["GET"])
@auth_free
def get_community_details(request):
    try:
        community_id = request.data.get("community_id")
        if not community_id:
            return Response(
                {
                    "success": False,
                    "message": "The community_id parameter is required.",
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        data = get_community_summary_data(community_id)
        return Response({"success": True, "data": data}, status=status.HTTP_200_OK)

    except Exception as e:
        print("Exception in get_community_details api :: ", e)
        return Response(
            {"success": False, "message": "An internal server error occurred."},
            status=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )


########### Get Community by Location  ############
@swagger_auto_schema(
    method="get",
    operation_id="get_communities_by_location",
    operation_summary="Get Communities by Location",
    operation_description="""
    Retrieve a list of communities filtered by state, district, or Tehsil ID. At least one location parameter must be provided.
    **Response dataset details:**
    ```
    {
        "success": true,
        "data": [
        {
        "id": 1,
        "name": "Community Name",
        "description": "Community description",
        "organization": "Organization Name",
        "member_count": 150,
        "location": "Location details"
        }
        ]
    }
    ```
    """,
    manual_parameters=[
        openapi.Parameter(
            "state_id",
            openapi.IN_QUERY,
            description="Filter by State ID",
            type=openapi.TYPE_INTEGER,
        ),
        openapi.Parameter(
            "district_id",
            openapi.IN_QUERY,
            description="Filter by District ID",
            type=openapi.TYPE_INTEGER,
        ),
        openapi.Parameter(
            "tehsil_id",
            openapi.IN_QUERY,
            description="Filter by Tehsil ID",
            type=openapi.TYPE_INTEGER,
        ),
    ],
    responses={
        200: openapi.Response(
            description="Success - Returns communities data",
            examples={
                "application/json": {
                    "success": True,
                    "data": [
                        {
                            "id": 1,
                            "name": "Community Name",
                            "description": "Community description",
                            "organization": "Organization Name",
                            "member_count": 150,
                            "location": "Location details",
                        }
                    ],
                }
            },
        ),
        400: openapi.Response(
            description="Bad Request - At least one location parameter (state_id, district_id, or tehsil_id) is required.",
            examples={
                "application/json": {
                    "success": False,
                    "message": "At least one location parameter (state_id, district_id, or tehsil_id) is required.",
                }
            },
        ),
        404: openapi.Response(
            description="Not Found - State not found OR District not found OR Tehsil not found",
            examples={
                "application/json": {"success": False, "message": "State not found"}
            },
        ),
        500: openapi.Response(
            description="Internal Server Error",
            examples={
                "application/json": {
                    "success": False,
                    "message": "An internal server error occurred.",
                }
            },
        ),
    },
    tags=["Community Engagement APIs"],
)
@api_view(["GET"])
@auth_free
def get_communities_by_location(request):
    try:
        print("Request query params:", request.query_params, request)
        state_id = request.query_params.get("state_id")
        district_id = request.query_params.get("district_id")
        block_id = request.query_params.get("block_id")

        state_name = district_name = block_name = ""

        if state_id:
            state = State.objects.filter(pk=state_id).first()
            state_name = state.state_name if state else ""

        if district_id:
            district = District.objects.filter(id=district_id).first()
            district_name = district.district_name if district else ""

        if block_id:
            block = Block.objects.filter(id=block_id).first()
            block_name = block.block_name if block else ""
        print(
            f"Fetching communities for State: '{state_id}', District: '{district_id}'"
        )
        data = get_communities(state_name, district_name, block_name)
        print(f"Communities found: {data}")
        return Response({"success": True, "data": data}, status=status.HTTP_200_OK)
    except Exception as e:
        print("Exception in get_communities_by_location:", e)
        return Response(
            {"success": False}, status=status.HTTP_500_INTERNAL_SERVER_ERROR
        )


############  Get Community by Lat Lon  ################


@swagger_auto_schema(
    method="get",
    operation_id="get_communities_by_lat_lon",
    operation_summary="Get Communities by lat lon.",
    operation_description="""
    Retrieve communities located at a specific latitude and longitude.
    **Response dataset details:**
    ```
    {
    "success": true,
    "data": [
        {
        "id": 1,
        "name": "Community Name",
        "description": "Community description",
        "organization": "Organization Name",
        "member_count": 150,
        "location": "Location details"
        }
    ]
    }
    ```
    """,
    manual_parameters=[
        openapi.Parameter(
            "latitude",
            openapi.IN_QUERY,
            description="Latitude value",
            type=openapi.TYPE_NUMBER,
            required=True,
        ),
        openapi.Parameter(
            "longitude",
            openapi.IN_QUERY,
            description="Longitude value",
            type=openapi.TYPE_NUMBER,
            required=True,
        ),
    ],
    responses={
        200: openapi.Response(
            description="Success - Returns communities data",
            examples={
                "application/json": {
                    "success": True,
                    "data": [
                        {
                            "id": 1,
                            "name": "Community Name",
                            "description": "Community description",
                            "organization": "Organization Name",
                            "member_count": 150,
                            "location": "Location details",
                        }
                    ],
                }
            },
        ),
        400: openapi.Response(
            description="Bad Request - Both 'latitude' and 'longitude' parameters are required.",
            examples={
                "application/json": {
                    "error": "Both 'latitude' and 'longitude' parameters are required."
                }
            },
        ),
        500: openapi.Response(
            description="Internal Server Error",
            examples={
                "application/json": {
                    "success": False,
                    "message": "An internal server error occurred.",
                }
            },
        ),
    },
    tags=["Community Engagement APIs"],
)
@api_view(["GET"])
@auth_free
def get_communities_by_lat_lon(request):
    try:
        lat = float(request.query_params.get("latitude"))
        lon = float(request.query_params.get("longitude"))
        if lat is None or lon is None:
            return Response(
                {"error": "Both 'latitude' and 'longitude' parameters are required."},
                status=status.HTTP_400_BAD_REQUEST,
            )
        if not (-90 <= lat <= 90 and -180 <= lon <= 180):
            return Response(
                {"error": "Latitude or longitude out of bounds."},
                status=status.HTTP_400_BAD_REQUEST,
            )
        location = get_location_info_by_lat_lon(lat, lon)
        state_name = location.get("State", "")
        district_name = location.get("District", "")
        tehsil_name = location.get("Tehsil", "")
        data = get_communities(state_name, district_name, tehsil_name)
        return Response({"success": True, "data": data}, status=status.HTTP_200_OK)
    except Exception as e:
        print("Exception in get_communities_by_lat_lon api :: ", e)
        return Response(
            {"success": False, "message": "An internal server error occurred."},
            status=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )


########### Get community for specific user  ############


@swagger_auto_schema(
    method="get",
    operation_id="get_community_by_user",
    operation_summary="Get Communities for a Specific User",
    operation_description="""
    Retrieve all communities that a specific user is a member of.
    **Response dataset details:**
    ```
    {
    "success": true,
    "data": [
        {
        "id": 1,
        "name": "Community Name",
        "description": "Community description",
        "organization": "Organization Name",
        "member_count": 150,
        "location": "Location details"
        }
    ]
    }
    ```
    """,
    manual_parameters=[
        openapi.Parameter(
            "number",
            openapi.IN_QUERY,
            description="User's contact number",
            type=openapi.TYPE_STRING,
            required=True,
        ),
    ],
    responses={
        200: openapi.Response(
            description="Success - Returns user's communities",
            examples={
                "application/json": {
                    "success": True,
                    "data": [
                        {
                            "id": 1,
                            "name": "Community Name",
                            "description": "Community description",
                            "organization": "Organization Name",
                            "member_count": 150,
                            "location": "Location details",
                        }
                    ],
                }
            },
        ),
        400: openapi.Response(
            description="Bad Request - Contact number is required.",
            examples={
                "application/json": {
                    "success": False,
                    "message": "Contact number is required.",
                }
            },
        ),
        404: openapi.Response(
            description="Not Found - User not found",
            examples={
                "application/json": {"success": False, "message": "User not found."}
            },
        ),
        500: openapi.Response(
            description="Internal Server Error",
            examples={
                "application/json": {
                    "success": False,
                    "message": "An internal server error occurred.",
                }
            },
        ),
    },
    tags=["Community Engagement APIs"],
)
@api_view(["GET"])
@auth_free
def get_community_by_user(request):
    try:
        data = []
        number = request.query_params.get("number")

        # Validate required parameter
        if not number:
            return Response(
                {"success": False, "message": "Contact number is required."},
                status=status.HTTP_400_BAD_REQUEST,
            )

        users = User.objects.filter(contact_number=number)
        if not users.exists():
            return Response(
                {"success": False, "message": "User not found."},
                status=status.HTTP_404_NOT_FOUND,
            )

        user = users.first()
        community_user_mappings = Community_user_mapping.objects.filter(user=user)
        for community_user_mapping in community_user_mappings:
            data.append(get_community_summary_data(community_user_mapping.community.id))

        return Response({"success": True, "data": data}, status=status.HTTP_200_OK)
    except Exception as e:
        print("Exception in get_community_by_user api :: ", e)
        return Response(
            {"success": False, "message": "An internal server error occurred."},
            status=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )


#################  Map Users to Community  ############################


@swagger_auto_schema(
    method="post",
    operation_id="map_users_to_community",
    operation_summary="Map Users to community",
    operation_description="""
    Upload a CSV file containing user phone numbers in the header to map them to a community project.
    **Response dataset details:**
    ```
    {
    "success": true,
    "message": "Users mapped to community successfully."
    }
    ```
    """,
    manual_parameters=[
        openapi.Parameter(
            "project_id",
            openapi.IN_FORM,
            description="ID of the Community Engagement project.",
            type=openapi.TYPE_INTEGER,
            required=True,
        ),
        openapi.Parameter(
            "file",
            openapi.IN_FORM,
            description="CSV file with user phone numbers as columns.",
            type=openapi.TYPE_FILE,
            required=True,
        ),
    ],
    responses={
        201: openapi.Response(
            description="Created - Users mapped to community successfully",
            examples={
                "application/json": {
                    "success": True,
                    "message": "Users mapped to community successfully.",
                }
            },
        ),
        400: openapi.Response(
            description="Bad Request - Project ID is required. OR Community Engagement project not found or not enabled OR No files were uploaded.",
            examples={
                "application/json": {
                    "detail": "Project ID is required. OR Community Engagement project not found or not enabled OR No files were uploaded."
                }
            },
        ),
        404: openapi.Response(
            description="Not Found - Community not found",
            examples={"application/json": {"detail": "Community not found."}},
        ),
        500: openapi.Response(
            description="Internal Server Error",
            examples={
                "application/json": {
                    "success": False,
                    "message": "An internal server error occurred.",
                }
            },
        ),
    },
    tags=["Community Engagement APIs"],
)
@api_view(["POST"])
@auth_free
@parser_classes([MultiPartParser, FormParser])
def map_users_to_community(request):
    try:
        project_id = request.data.get("project_id")
        if not project_id:
            return Response(
                {"detail": "Project ID is required."},
                status=status.HTTP_400_BAD_REQUEST,
            )

        try:
            project = Project.objects.get(
                id=project_id, app_type=AppType.COMMUNITY_ENGAGEMENT, enabled=True
            )
        except Project.DoesNotExist:
            return Response(
                {"detail": "Community Engagement project not found or not enabled."},
                status=status.HTTP_400_BAD_REQUEST,
            )

        files = []

        if "file" in request.FILES:
            files.append(request.FILES["file"])

        if "files[]" in request.FILES:
            print("Found multiple files with 'files[]'")
            file_list = request.FILES.getlist("files[]")
            print(f"Number of files in 'files[]': {len(file_list)}")
            for f in file_list:
                print(f"  - {f.name}")
            files.extend(file_list)

        if not files:
            return Response(
                {"detail": "No files were uploaded."},
                status=status.HTTP_400_BAD_REQUEST,
            )

        for uploaded_file in files:
            print(uploaded_file.name)

            df = pd.read_csv(uploaded_file)
            numbers = df.columns.tolist()

            users = []
            for number in numbers:
                user, created = User.objects.get_or_create(
                    contact_number=number, defaults={"username": number}
                )
                users.append(user)

            try:
                community = Community.objects.get(project=project)
            except Community.DoesNotExist:
                return Response(
                    {"detail": "Community not found."},
                    status=status.HTTP_404_NOT_FOUND,
                )

            for user_obj in users:
                Community_user_mapping.objects.get_or_create(
                    community_id=community.id, user=user_obj
                )

        return Response(
            {"success": True, "message": "Users mapped to community successfully."},
            status=status.HTTP_201_CREATED,
        )
    except Exception as e:
        print("Exception in map_users_to_community api :: ", e)
        return Response(
            {"success": False, "message": "An internal server error occurred."},
            status=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )


##################################################################
##  Add User to the community
##################################################################


@swagger_auto_schema(
    method="post",
    operation_id="add_user_to_community",
    operation_summary="Add a Single User to a Community",
    operation_description="""
    Add user to the community.
    
    **Response dataset details:**
    ```
    {
        "success": true,
        "message": "User added to community successfully."
    }
    ```
    """,
    manual_parameters=[
        openapi.Parameter(
            "community_id",
            openapi.IN_FORM,
            description="Community identifier",
            type=openapi.TYPE_INTEGER,
            required=True,
        ),
        openapi.Parameter(
            "number",
            openapi.IN_FORM,
            description="User's contact number",
            type=openapi.TYPE_STRING,
            required=True,
        ),
    ],
    responses={
        201: openapi.Response(
            description="Success - User added to community successfully",
            examples={
                "application/json": {
                    "success": True,
                    "message": "User added to community successfully.",
                }
            },
        ),
        200: openapi.Response(
            description="Success - User was already in the community",
            examples={
                "application/json": {
                    "success": True,
                    "message": "User was already in the community.",
                }
            },
        ),
        400: openapi.Response(
            description="Bad Request",
            examples={"application/json": {"detail": "Community ID is required."}},
        ),
        404: openapi.Response(
            description="Not Found",
            examples={"application/json": {"detail": "Community does not exist."}},
        ),
        500: openapi.Response(
            description="Internal Server Error",
            examples={
                "application/json": {"detail": "An internal server error occurred."}
            },
        ),
    },
    tags=["Community Engagement APIs"],
)
@api_view(["POST"])
@auth_free
@parser_classes([MultiPartParser, FormParser])
def add_user_to_community(request):
    try:
        community_id = request.data.get("community_id")
        number = request.data.get("number")

        if not community_id:
            return Response(
                {"detail": "Community ID is required."},
                status=status.HTTP_400_BAD_REQUEST,
            )

        if not number:
            return Response(
                {"detail": "User contact number is required."},
                status=status.HTTP_400_BAD_REQUEST,
            )

        # Check if community exists
        if not Community.objects.filter(id=community_id).exists():
            return Response(
                {"detail": "Community does not exist."},
                status=status.HTTP_404_NOT_FOUND,
            )

        # Check if user already exists in this community
        user, user_created = User.objects.get_or_create(
            contact_number=number, defaults={"username": number}
        )
        mapping, mapping_created = Community_user_mapping.objects.get_or_create(
            community_id=community_id, user=user
        )

        if mapping_created:
            return Response(
                {"success": True, "message": "User added to community successfully."},
                status=status.HTTP_201_CREATED,
            )
        else:
            return Response(
                {"success": True, "message": "User was already in the community."},
                status=status.HTTP_200_OK,
            )

    except Exception as e:
        print("Exception in add_user_to_community api :: ", e)
        return Response(
            {"detail": "An internal server error occurred."},
            status=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )


#######################################################
## Check if user exists in community
#######################################################


@swagger_auto_schema(
    method="post",
    operation_id="is_user_in_community",
    operation_summary="Is user in community",
    operation_description="""
    Checks if a user is part of any community. 
    
    - If user is in communities: Returns list of their communities with summary data
    
    **Response dataset details:**
    ```
    {
        "success": true,
        "data": {
            "is_in_community": true,
            "data_type": "community",
            "data": [
                {
                    "community_id": 4,
                    "name": "NREGASathi",
                    "description": "NREGA Companions",
                    "organization": "CFPT"
                }
            ],
            "misc": {
                "last_accessed_community_id": 4
            }
        }
    }
    ```
    """,
    request_body=openapi.Schema(
        type=openapi.TYPE_OBJECT,
        required=["number"],
        properties={
            "number": openapi.Schema(
                type=openapi.TYPE_STRING, description="User's contact number"
            )
        },
    ),
    responses={
        200: openapi.Response(
            description="Success",
            examples={
                "application/json": {
                    "success": True,
                    "data": {
                        "is_in_community": True,
                        "data_type": "community",
                        "data": [
                            {
                                "community_id": 4,
                                "name": "NREGASathi",
                                "description": "NREGA Companions",
                                "organization": "CFPT",
                            }
                        ],
                        "misc": {"last_accessed_community_id": 4},
                    },
                }
            },
        ),
        400: openapi.Response(
            description="Bad Request - Either 'number' field is missing or it is empty.",
            examples={
                "application/json": {
                    "success": False,
                    "message": "Either 'number' field is missing or it is empty.",
                }
            },
        ),
        404: openapi.Response(
            description="Not Found - User doesnot Exist.",
            examples={
                "application/json": {"success": False, "message": "User doesnot Exist."}
            },
        ),
        500: openapi.Response(
            description="Internal Server Error",
            examples={
                "application/json": {
                    "success": False,
                    "message": "An internal server error occurred.",
                }
            },
        ),
    },
    tags=["Community Engagement APIs"],
)
@api_view(["POST"])
@auth_free
def is_user_in_community(request):
    try:
        number = request.data.get("number")
        if not number:
            return Response(
                {
                    "success": False,
                    "message": "Either 'number' field is missing or it is empty.",
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        user_objs = User.objects.filter(contact_number=number)
        data = {}

        if user_objs.exists():
            user = user_objs.first()
            community_user_mapping_qs = Community_user_mapping.objects.filter(user=user)

            if community_user_mapping_qs.exists():
                communities_list = []
                last_accessed_community_id = ""
                for mapping in community_user_mapping_qs:
                    communities_list.append(
                        get_community_summary_data(mapping.community.id)
                    )
                    if mapping.is_last_accessed_community:
                        last_accessed_community_id = mapping.community.id

                data["is_in_community"] = True
                data["data_type"] = "community"
                data["data"] = communities_list
                data["misc"] = {
                    "last_accessed_community_id": last_accessed_community_id
                }
                return Response(
                    {"success": True, "data": data}, status=status.HTTP_200_OK
                )
        else:
            return Response(
                {"success": False, "message": "User doesnot Exist"},
                status=status.HTTP_404_NOT_FOUND,
            )

        state_ids_with_community = (
            Location.objects.filter(communities__isnull=False)
            .values_list("state_id", flat=True)
            .distinct()
        )
        states = State.objects.filter(pk__in=state_ids_with_community).order_by(
            "state_name"
        )
        data["is_in_community"] = False
        data["data_type"] = "state"
        data["data"] = [{"id": state.pk, "name": state.state_name} for state in states]
        data["misc"] = {}
        return Response({"success": True, "data": data}, status=status.HTTP_200_OK)

    except Exception as e:
        print("Exception in is_user_in_community API ::", e)
        return Response(
            {"success": False, "message": "An internal server error occurred."},
            status=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )


######### Get District with community  #########


@swagger_auto_schema(
    method="get",
    operation_id="get_districts_with_community",
    operation_summary="Get Districts with Communities",
    operation_description="""
    Checks if a state has any communities and returns its districts. 
    
    - If state has communities: Returns list of districts with community data
    
    **Response dataset details:**
    ```
    {
        "success": true,
        "data": [
            {
                "id": 63,
                "name": "Deoghar"
            },
            {
                "id": 75,
                "name": "Dumka"
            },
            {
                "id": 66,
                "name": "Pakur"
            }
        ]
    }
    ```
    """,
    manual_parameters=[
        openapi.Parameter(
            "state_id",
            openapi.IN_QUERY,
            required=True,
            type=openapi.TYPE_INTEGER,
            description="ID of the state to filter districts",
        )
    ],
    responses={
        200: openapi.Response(
            description="Success",
            examples={
                "application/json": {
                    "success": True,
                    "data": [
                        {"id": 63, "name": "Deoghar"},
                        {"id": 75, "name": "Dumka"},
                        {"id": 66, "name": "Pakur"},
                    ],
                }
            },
        ),
        400: openapi.Response(
            description="Bad Request - State ID is required.",
            examples={
                "application/json": {
                    "success": False,
                    "message": "State ID is required.",
                }
            },
        ),
        404: openapi.Response(
            description="Not Found - State does not exist.",
            examples={
                "application/json": {
                    "success": False,
                    "message": "State does not exist.",
                }
            },
        ),
        500: openapi.Response(
            description="Internal Server Error",
            examples={
                "application/json": {
                    "success": False,
                    "message": "An internal server error occurred.",
                }
            },
        ),
    },
    tags=["Community Engagement APIs"],
)
@api_view(["GET"])
@auth_free
def get_districts_with_community(request):
    try:
        state_id = request.query_params.get("state_id").strip()
        if not state_id:
            return Response(
                {"success": False, "message": "State ID is required."},
                status=status.HTTP_400_BAD_REQUEST,
            )

        # Check if state exists
        state_obj = State.objects.filter(pk=state_id).first()
        if not state_obj:
            return Response(
                {"success": False, "message": "State does not exist."},
                status=status.HTTP_404_NOT_FOUND,
            )

        # Get districts with communities for the state
        district_ids = Location.objects.filter(state=state_obj).values_list(
            "district_id", flat=True
        )
        districts = District.objects.filter(pk__in=district_ids).order_by(
            "district_name"
        )

        districts_data = [
            {"id": district.pk, "name": district.district_name}
            for district in districts
        ]
        return Response(
            {"success": True, "data": districts_data}, status=status.HTTP_200_OK
        )

    except Exception as e:
        print("Exception in get_districts_with_community API ::", e)
        return Response(
            {"success": False, "message": "An internal server error occurred."},
            status=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )


##############################################
###### Get Tehsils with Community
##############################################


@swagger_auto_schema(
    method="get",
    operation_id="get_tehsils_with_community",
    operation_summary="Get Tehsils with Communities",
    operation_description="""
    Checks if a district has any communities and returns its tehsils. 
    
    - If district has communities: Returns list of tehsils with community data
    
    **Response dataset details:**
    ```
    {
        "success": true,
        "data": [
            {
                "id": 1,
                "name": "Tehsil A"
            },
            {
                "id": 2,
                "name": "Tehsil B"
            }
        ]
    }
    ```
    """,
    manual_parameters=[
        openapi.Parameter(
            "district_id",
            openapi.IN_QUERY,
            required=True,
            type=openapi.TYPE_INTEGER,
            description="ID of the district to filter tehsils",
        )
    ],
    responses={
        200: openapi.Response(
            description="Success - It will return json with data.",
            examples={
                "application/json": {
                    "success": True,
                    "data": [
                        {"id": 1, "name": "Tehsil A"},
                        {"id": 2, "name": "Tehsil B"},
                    ],
                }
            },
        ),
        400: openapi.Response(
            description="Bad Request - District ID is required.",
            examples={
                "application/json": {
                    "success": False,
                    "message": "District ID is required.",
                }
            },
        ),
        404: openapi.Response(
            description="Not Found - District does not exist.",
            examples={
                "application/json": {
                    "success": False,
                    "message": "District does not exist.",
                }
            },
        ),
        500: openapi.Response(
            description="Internal Server Error",
            examples={
                "application/json": {
                    "success": False,
                    "message": "An internal server error occurred.",
                }
            },
        ),
    },
    tags=["Community Engagement APIs"],
)
@api_view(["GET"])
@auth_free
def get_tehsils_with_community(request):
    try:
        district_id = request.query_params.get("district_id").strip()
        if not district_id:
            return Response(
                {"success": False, "message": "District ID is required."},
                status=status.HTTP_400_BAD_REQUEST,
            )

        # Check if district exists
        district_obj = District.objects.filter(pk=district_id).first()
        if not district_obj:
            return Response(
                {"success": False, "message": "District does not exist."},
                status=status.HTTP_404_NOT_FOUND,
            )

        # Get tehsils with communities for the district
        block_ids = Location.objects.filter(district=district_obj).values_list(
            "block_id", flat=True
        )
        blocks = Block.objects.filter(pk__in=block_ids).order_by("block_name")

        blocks_data = [{"id": block.pk, "name": block.block_name} for block in blocks]
        return Response(
            {"success": True, "data": blocks_data}, status=status.HTTP_200_OK
        )

    except Exception as e:
        print("Exception in get_tehsils_with_community API ::", e)
        return Response(
            {"success": False, "message": "An internal server error occurred."},
            status=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )


#####################################################
#########


@swagger_auto_schema(
    method="post",
    operation_id="update_last_accessed_community",
    operation_summary="Update User's Last Accessed Community",
    operation_description="""
    Update the last accessed community for a user and mark all other communities as not last accessed.",
    **Response dataset details:**
    ```
    {
        "success": true,
        "message": "Last accessed community updated successfully."
    }
    ```
    """,
    request_body=openapi.Schema(
        type=openapi.TYPE_OBJECT,
        required=["user_id", "community_id"],
        properties={
            "user_id": openapi.Schema(type=openapi.TYPE_INTEGER),
            "community_id": openapi.Schema(type=openapi.TYPE_INTEGER),
        },
    ),
    responses={
        200: openapi.Response(
            description="Success - Last accessed community updated successfully.",
            examples={
                "application/json": {
                    "success": True,
                    "message": "Last accessed community updated successfully.",
                }
            },
        ),
        400: openapi.Response(
            description="Bad Request - user_id and community_id are required.",
            examples={
                "application/json": {
                    "success": False,
                    "error": "user_id and community_id are required.",
                }
            },
        ),
        500: openapi.Response(
            description="Internal Server Error",
            examples={
                "application/json": {
                    "success": False,
                    "error": "An internal server error occurred.",
                }
            },
        ),
    },
    tags=["Community Engagement APIs"],
)
@api_view(["POST"])
@auth_free
def update_last_accessed_community(request):
    try:
        user_id = request.data.get("user_id")
        community_id = request.data.get("community_id")

        if not user_id or not community_id:
            return Response(
                {"success": False, "error": "user_id and community_id are required."},
                status=status.HTTP_400_BAD_REQUEST,
            )

        mapping, created = Community_user_mapping.objects.update_or_create(
            user_id=user_id,
            community_id=community_id,
            defaults={"is_last_accessed_community": True},
        )

        Community_user_mapping.objects.filter(user_id=user_id).exclude(
            pk=mapping.pk
        ).update(is_last_accessed_community=False)
        return Response(
            {
                "success": True,
                "message": "Last accessed community updated successfully.",
            },
            status=status.HTTP_200_OK,
        )
    except Exception as e:
        print("Exception in update_last_accessed_community API :: ", e)
        return Response(
            {"success": False, "error": "An internal server error occurred."},
            status=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )


##########################################
##### Get Items by Community
##########################################


@swagger_auto_schema(
    method="get",
    operation_id="get_items_by_community",
    operation_summary="Get Items by Community",
    operation_description="""
    Retrieve paginated items from a community with optional filtering by item type and state.",
    **Response dataset details:**
    ```
    {
        "success": true,
        "data": [
            {
                "id": 7,
                "number": "917702828811",
                "title": "Asset Demand - 10 Sep 2025, 11:50 AM",
                "item_type": "ASSET_DEMAND",
                "state": "UNMODERATED",
                "created_at": "2025-09-10 06:20:52",
                "latitude": 24.9175964,
                "longitude": 86.2940056,
                "images": [
                    "https://communityengagementstack.s3.ap-south-1.amazonaws.com/images/7dd5c2e0-7e5e-400d-9b8d-19e243642443.jpeg"
                ],
                "audios": [
                    "https://communityengagementstack.s3.ap-south-1.amazonaws.com/audios/bacfd6c6-ec9b-4706-8265-7a19ecece286.wav"
                ]
            }
        ],
        "total": 1,
        "limit": 5,
        "offset": 0,
        "has_more": true
    }
    ```
    """,
    manual_parameters=[
        openapi.Parameter(
            "community_id",
            openapi.IN_QUERY,
            required=True,
            type=openapi.TYPE_INTEGER,
            description="ID of the community",
        ),
        openapi.Parameter(
            "item_type",
            openapi.IN_QUERY,
            type=openapi.TYPE_STRING,
            description="Type of items to filter",
        ),
        openapi.Parameter(
            "item_state",
            openapi.IN_QUERY,
            type=openapi.TYPE_STRING,
            description="State of items to filter (requires item_type)",
        ),
        openapi.Parameter(
            "limit",
            openapi.IN_QUERY,
            type=openapi.TYPE_INTEGER,
            default=10,
            description="Number of items per page",
        ),
        openapi.Parameter(
            "offset",
            openapi.IN_QUERY,
            type=openapi.TYPE_INTEGER,
            default=0,
            description="Number of items to skip",
        ),
    ],
    responses={
        200: openapi.Response(
            description="Success - Returns items data",
            examples={
                "application/json": {
                    "success": True,
                    "data": [
                        {
                            "id": 7,
                            "number": "917702828811",
                            "title": "Asset Demand - 10 Sep 2025, 11:50 AM",
                            "item_type": "ASSET_DEMAND",
                            "state": "UNMODERATED",
                            "created_at": "2025-09-10 06:20:52",
                            "latitude": 24.9175964,
                            "longitude": 86.2940056,
                            "images": [
                                "https://communityengagementstack.s3.ap-south-1.amazonaws.com/images/7dd5c2e0-7e5e-400d-9b8d-19e243642443.jpeg"
                            ],
                            "audios": [
                                "https://communityengagementstack.s3.ap-south-1.amazonaws.com/audios/bacfd6c6-ec9b-4706-8265-7a19ecece286.wav"
                            ],
                        }
                    ],
                    "total": 1,
                    "limit": 5,
                    "offset": 0,
                    "has_more": True,
                }
            },
        ),
        400: openapi.Response(
            description="Bad Request - Missing 'community_id' in query parameters.",
            examples={
                "application/json": {
                    "success": False,
                    "message": "Missing 'community_id' in query parameters.",
                }
            },
        ),
        500: openapi.Response(
            description="Internal Server Error",
            examples={
                "application/json": {
                    "success": False,
                    "message": "Internal server error.",
                }
            },
        ),
    },
    tags=["Community Engagement APIs"],
)
@api_view(["GET"])
@auth_free
def get_items_by_community(request):
    try:
        community_id = request.query_params.get("community_id")
        if not community_id:
            return Response(
                {
                    "success": False,
                    "message": "Missing 'community_id' in query parameters.",
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        item_type = request.query_params.get("item_type")
        item_state = request.query_params.get("item_state")

        limit = int(request.query_params.get("limit", 10))
        offset = int(request.query_params.get("offset", 0))

        items_qs = Item.objects.filter(community_id=community_id)

        if item_type:
            items_qs = items_qs.filter(item_type=item_type)

            if item_state:
                valid_states = [
                    state.value for state in ITEM_TYPE_STATE_MAP.get(item_type, [])
                ]
                if item_state not in valid_states:
                    return Response(
                        {
                            "success": False,
                            "message": f"Invalid item_state '{item_state}' for item_type '{item_type}'.",
                        },
                        status=status.HTTP_400_BAD_REQUEST,
                    )
                items_qs = items_qs.filter(state=item_state)

        elif item_state:
            return Response(
                {
                    "success": False,
                    "message": "Cannot filter by item_state without specifying item_type.",
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        items_qs = items_qs.order_by("-created_at")
        total_count = items_qs.count()
        paginated_items = items_qs[offset : offset + limit]

        data = []
        for item in paginated_items:
            lat, lon = None, None

            if item.coordinates:
                try:
                    coord = json.loads(item.coordinates)
                except json.JSONDecodeError:
                    try:
                        coord = ast.literal_eval(item.coordinates)
                    except Exception as e:
                        print(f"Error parsing coordinates for item {item.id}: {e}")
                        coord = {}

                if isinstance(coord, dict):
                    lat = (
                        float(coord["lat"])
                        if "lat" in coord and coord["lat"] is not None
                        else None
                    )
                    lon = (
                        float(coord["lon"])
                        if "lon" in coord and coord["lon"] is not None
                        else None
                    )

            images = list(
                item.media.filter(media_type=Media_type.IMAGE).values_list(
                    "media_path", flat=True
                )
            )
            audios = list(
                item.media.filter(media_type=Media_type.AUDIO).values_list(
                    "media_path", flat=True
                )
            )

            data.append(
                {
                    "id": item.id,
                    "number": item.user.contact_number,
                    "title": item.title,
                    "item_type": item.item_type,
                    "state": item.state,
                    "created_at": item.created_at.strftime("%Y-%m-%d %H:%M:%S"),
                    "latitude": lat,
                    "longitude": lon,
                    "images": images,
                    "audios": audios,
                }
            )

        return Response(
            {
                "success": True,
                "data": data,
                "total": total_count,
                "limit": limit,
                "offset": offset,
                "has_more": offset + limit < total_count,
            },
            status=status.HTTP_200_OK,
        )

    except Exception as e:
        print("Exception in get_items_by_community API:", str(e))
        return Response(
            {"success": False, "message": "Internal server error."},
            status=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )


#################################################
#########  Get Items Status
#################################################


@swagger_auto_schema(
    method="get",
    operation_id="get_items_status",
    operation_summary="Get Status of a User's Items",
    operation_description="""
    Retrieve the status and details of items submitted by a user, with optional filtering by bot, community, and item type.",
    **Response dataset details:**
    ```
    {
        "success": true,
        "data": [
            {
                "id": 8,
                "title": "Grievance - 11 Sep 2025, 1:44 PM",
                "transcription": "",
                "status": "UNMODERATED"
            }
        ]
    }
    ```
    """,
    manual_parameters=[
        openapi.Parameter(
            "number",
            openapi.IN_QUERY,
            description="User's contact number",
            required=True,
            type=openapi.TYPE_STRING,
        ),
        openapi.Parameter(
            "bot_id",
            openapi.IN_QUERY,
            type=openapi.TYPE_INTEGER,
            description="Filter by bot ID",
        ),
        openapi.Parameter(
            "community_id",
            openapi.IN_QUERY,
            type=openapi.TYPE_INTEGER,
            description="Filter by community ID",
        ),
        openapi.Parameter(
            "asset_demand_only",
            openapi.IN_QUERY,
            type=openapi.TYPE_BOOLEAN,
            default=False,
            description="Filter only asset demand items",
        ),
    ],
    responses={
        200: openapi.Response(
            description="Success - Returns user's items status data",
            examples={
                "application/json": {
                    "success": True,
                    "data": [
                        {
                            "id": 8,
                            "title": "Grievance - 11 Sep 2025, 1:44 PM",
                            "transcription": "",
                            "status": "UNMODERATED",
                        }
                    ],
                }
            },
        ),
        400: openapi.Response(
            description="Bad Request - contact_number is required.",
            examples={
                "application/json": {
                    "success": False,
                    "message": "'contact_number' is required.",
                }
            },
        ),
        404: openapi.Response(
            description="Not Found - User not found",
            examples={
                "application/json": {"success": False, "message": "User not found."}
            },
        ),
        500: openapi.Response(
            description="Internal Server Error",
            examples={
                "application/json": {
                    "success": False,
                    "message": "Internal server error.",
                }
            },
        ),
    },
    tags=["Community Engagement APIs"],
)
@api_view(["GET"])
@auth_free
def get_items_status(request):
    try:
        contact_number = request.query_params.get("number")
        bot_id = request.query_params.get("bot_id")
        community_id = request.query_params.get("community_id")
        asset_demand_only = (
            request.query_params.get("asset_demand_only", "false").lower() == "true"
        )

        if not contact_number:
            return Response(
                {"success": False, "message": "'contact_number' is required."},
                status=status.HTTP_400_BAD_REQUEST,
            )

        if not bot_id and not community_id:
            return Response(
                {
                    "success": False,
                    "message": "Either 'bot_id' or 'community_id' must be provided.",
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        user = User.objects.filter(contact_number=contact_number).first()
        if not user:
            return Response(
                {"success": False, "message": "User not found."},
                status=status.HTTP_404_NOT_FOUND,
            )

        items_qs = Item.objects.filter(user=user)

        if asset_demand_only:
            items_qs = items_qs.filter(item_type=Item_type.ASSET_DEMAND)

        if community_id:
            items_qs = items_qs.filter(community_id=community_id)

        if bot_id:
            items_qs = items_qs.filter(community__bot_id=bot_id)

        data = [
            {
                "id": item.id,
                "title": item.title,
                "transcription": item.transcript,
                "status": item.state,
            }
            for item in items_qs
        ]

        return Response({"success": True, "data": data}, status=status.HTTP_200_OK)

    except Exception as e:
        print("Exception in get_items_status API ::", e)
        return Response(
            {"success": False, "message": "Internal server error."},
            status=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )


@api_view(["POST"])
@schema(None)
@auth_free
def create_community(request):
    try:
        name = request.data.get("name")
        organization_id = request.data.get("organization")

        if not name or not organization_id:
            return Response(
                {"success": False, "error": "name and organization are required."},
                status=status.HTTP_400_BAD_REQUEST,
            )

        # Prepare project data
        project_data = {
            "name": name,
            "description": request.data.get("description"),
            "organization_id": organization_id,
            "state_id": request.data.get("state"),
            "district_id": request.data.get("district"),
            "block_id": request.data.get("block"),
            "created_by": request.data.get("created_by"),
            "updated_by": request.data.get("updated_by"),
            "app_type": request.data.get("app_type", "community_engagement"),
        }

        # Create project and community
        community = create_community_for_project(project_data)

        return Response(
            {
                "success": True,
                "message": "Community created successfully.",
                "data": {
                    "community_id": community.id,
                    "community_name": community.project.name,
                },
            },
            status=status.HTTP_201_CREATED,
        )

    except Exception as e:
        print("Exception in create_community API :: ", e)
        return Response(
            {"success": False, "error": "An internal server error occurred."},
            status=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )
