from django.http import HttpRequest
from rest_framework import status
from rest_framework.decorators import api_view
from rest_framework.request import Request
from rest_framework.response import Response
import boto3
import uuid
import pandas as pd

from .models import Community, Item, Community_user_mapping, Media, Location
from .utils import get_media_type, get_community_summary_data, get_communities
from geoadmin.models import State, District, Block
from projects.models import Project, AppType
from users.models import User
from utilities.auth_utils import auth_free
from nrm_app.settings import S3_BUCKET, S3_ACCESS_KEY, S3_SECRET_ACCESS_KEY, S3_REGION


@api_view(["POST"])
@auth_free
def create_item(request):
    try:
        title = request.data.get("title")
        transcript = request.data.get("transcript")
        category_id = request.data.get("category_id")
        rating = request.data.get("rating")
        item_type = request.data.get("item_type")
        coordinates = request.data.get("coordinates")
        state = request.data.get("state")
        user_id = request.data.get("user_id")
        community_id = request.data.get("community_id")

        item = Item(title=title, transcript=transcript, category_id=category_id, rating=rating, item_type=item_type, coordinates=coordinates, state=state, user_id=user_id, community_id=community_id)
        item.save()

        return Response({"success": True, "item_id": item.id}, status=status.HTTP_201_CREATED)
    except Exception as e:
        print("Exception in create_item api :: ", e)
        return Response({"success": False}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(["POST"])
@auth_free
def attach_media_to_item(request):
    try:
        s3_client = boto3.client('s3',
                                 aws_access_key_id=S3_ACCESS_KEY,
                                 aws_secret_access_key=S3_SECRET_ACCESS_KEY,
                                 region_name=S3_REGION
                                 )

        item_id = request.data.get('item_id')
        item = Item.objects.get(id=item_id)

        user_id = request.data.get('user_id')

        files = request.data.get('files', '')

        for media_key, media_files in files.items():
            media_type = get_media_type(media_key)
            if media_type and len(media_files)>0:
                for media_file in media_files:
                    media_file_name = media_file.name
                    media_file_extension = media_file_name.split(".")[-1]
                    key = media_key + "/" + str(uuid.uuid4()) + "." + media_file_extension
                    s3_client.upload_fileobj(media_file, S3_BUCKET, key)
                    media_path = S3_BUCKET + "/" + key
                    media_obj = Media.objects.create(user_id=user_id, media_type=media_type, media_path=media_path)
                    item.media.add(media_obj)

        return Response({"success": True}, status=status.HTTP_201_CREATED)
    except Exception as e:
        print("Exception in attach_media_to_item api :: ", e)
        return Response({"success": False}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(["GET"])
@auth_free
def get_community_details(request):
    try:
        data = get_community_summary_data(request.data.get('community_id'))
        return Response({"success": True, "data": data}, status=status.HTTP_201_CREATED)
    except Exception as e:
        print("Exception in get_community_details api :: ", e)
        return Response({"success": False}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(["GET"])
@auth_free
def get_communities_by_location(request):
    try:
        state_name    = (request.query_params.get("state") or "").strip()
        district_name = (request.query_params.get("district") or "").strip()
        block_name    = (request.query_params.get("block") or "").strip()
        data = get_communities(state_name, district_name, block_name)
        return Response({"success": True, "data": data}, status=status.HTTP_200_OK)
    except Exception as e:
        print("Exception in get_communities_by_location:", e)
        return Response({"success": False}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(["GET"])
@auth_free
def get_communities_by_lat_lon(request):
    try:
        from public_api.views import get_location_info_by_lat_lon
        lat = float(request.query_params.get("latitude"))
        lon = float(request.query_params.get("longitude"))
        if not (-90 <= lat <= 90 and -180 <= lon <= 180):
            return Response({"error": "Latitude or longitude out of bounds."}, status=400)
        location = get_location_info_by_lat_lon(lat, lon)
        state_name = location.get("State", "")
        district_name = location.get("District", "")
        block_name = location.get("Tehsil", "")
        data = get_communities(state_name, district_name, block_name)
        return Response({"success": True, "data": data}, status=status.HTTP_200_OK)
    except Exception as e:
        print("Exception in get_communities_by_lat_lon api :: ", e)
        return Response({"success": False}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(["GET"])
@auth_free
def get_community_by_user(request):
    try:
        data = []
        number = request.query_params.get('number')
        users = User.objects.filter(contact_number=number)
        if users.exists():
            user = users[0]
            community_user_mappings = Community_user_mapping.objects.filter(user=user)
            for community_user_mapping in community_user_mappings:
                data.append(get_community_summary_data(community_user_mapping.community.id))

        return Response({"success": True, "data": data}, status=status.HTTP_201_CREATED)
    except Exception as e:
        print("Exception in get_community_by_user api :: ", e)
        return Response({"success": False}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(["POST"])
@auth_free
def map_users_to_community(request):
    try:
        project_id = request.data.get("project_id")
        if not project_id:
            return Response(
                {"detail": "Project ID is required."},
                status=status.HTTP_400_BAD_REQUEST,
            )

        # Get project and check if it's a community_engagement project and enabled
        try:
            project = Project.objects.get(
                id=project_id, app_type=AppType.COMMUNITY_ENGAGEMENT, enabled=True
            )
        except Project.DoesNotExist:
            return Response(
                {"detail": "Community Engagement project not found or not enabled."},
                status=status.HTTP_400_BAD_REQUEST,
            )

        # Check if we have files in the request
        files = []

        # Handle single file upload case
        if "file" in request.FILES:
            files.append(request.FILES["file"])

        # Handle multiple files upload case
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
                user, created = User.objects.get_or_create(contact_number=number, defaults={'username': number})
                users.append(user)

            community = Community.objects.get(project=project)
            for user_obj in users:
                Community_user_mapping.objects.get_or_create(community_id=community.id, user=user_obj)
        return Response({"success": True}, status=status.HTTP_201_CREATED)
    except Exception as e:
        print("Exception in map_users_to_community api :: ", e)
        return Response({"success": False}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(["POST"])
@auth_free
def add_user_to_community(request):
    try:
        community_id = request.data.get("community_id")
        if not community_id:
            return Response(
                {"detail": "Community ID is required."},
                status=status.HTTP_400_BAD_REQUEST,
            )

        number = request.data.get("number")
        user, created = User.objects.get_or_create(contact_number=number, defaults={'username': number})
        Community_user_mapping.objects.get_or_create(community_id=community_id, user=user)
        return Response({"success": True}, status=status.HTTP_201_CREATED)
    except Exception as e:
        print("Exception in add_user_to_community api :: ", e)
        return Response({"success": False}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(["POST"])
@auth_free
def is_user_in_community(request):
    try:
        number = request.data.get("number")
        if not number:
            return Response(
                {
                    "success": False,
                    "message": "Either 'number' field is missing or it is empty."
                },
                status=status.HTTP_400_BAD_REQUEST
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
                    communities_list.append(get_community_summary_data(mapping.community.id))
                    if mapping.is_last_accessed_community:
                        last_accessed_community_id = mapping.community.id

                data["is_in_community"] = True
                data["data_type"] = "community"
                data["data"] = communities_list
                data["misc"] = {"last_accessed_community_id": last_accessed_community_id}
                return Response({"success": True, "data": data}, status=status.HTTP_200_OK)

        state_ids_with_community = Location.objects.filter(communities__isnull=False).values_list('state_id', flat=True).distinct()
        states = State.objects.filter(pk__in=state_ids_with_community).order_by('state_name')
        data["is_in_community"] = False
        data["data_type"] = "state"
        data["data"] = [{"id": state.pk, "name": state.state_name} for state in states]
        data["misc"] = {}
        return Response({"success": True, "data": data}, status=status.HTTP_200_OK)
    except Exception as e:
        print("Exception in is_user_in_community API ::", e)
        return Response({"success": False}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(["GET"])
@auth_free
def get_districts_with_community(request):
    try:
        state_id = request.query_params.get("state_id").strip()
        state_obj = State.objects.filter(pk=state_id).first()

        if state_obj:
            district_ids = Location.objects.filter(state=state_obj).values_list("district_id", flat=True)
            districts = District.objects.filter(pk__in=district_ids).order_by("district_name")
        else:
            districts = District.objects.none()

        districts_data = [{"id": district.pk, "name": district.district_name} for district in districts]
        return Response({"success": True, "data": districts_data}, status=status.HTTP_200_OK)
    except Exception as e:
        print("Exception in get_districts_with_community API ::", e)
        return Response({"success": False}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(["GET"])
@auth_free
def get_blocks_with_community(request):
    try:
        district_id = request.query_params.get("district_id").strip()
        district_obj = District.objects.filter(pk=district_id).first()

        if district_obj:
            block_ids = Location.objects.filter(district=district_obj).values_list("block_id", flat=True)
            blocks = Block.objects.filter(pk__in=block_ids).order_by("block_name")
        else:
            blocks = Block.objects.none()

        blocks_data = [{"id": block.pk, "name": block.block_name} for block in blocks]
        return Response({"success": True, "data": blocks_data}, status=status.HTTP_200_OK)
    except Exception as e:
        print("Exception in get_districts_with_community API ::", e)
        return Response({"success": False}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


# @api_view(["GET"])
# @auth_free
# def get_items_by_community(request):
#     try:
#         data = []
#         community_id = request.query_params.get("community_id")
#         if not community_id:
#             return Response(
#                 {"success": False, "message": "Missing 'community_id' in query parameters."},
#                 status=status.HTTP_400_BAD_REQUEST
#             )
#
#         items = Item.objects.filter(community_id=community_id)
#         for item in items:
#             item_dict = {'id': item.id,
#                          'number': item.user.contact_number,
#                          'title': item.title,
#                          'item_type': item.item_type}
#             data.append(item_dict)
#         return Response({"success": True, "data": data}, status=status.HTTP_200_OK)
#     except Exception as e:
#         print("Exception in get_items_by_community API:", str(e))
#         return Response(
#             {"success": False, "message": "Internal server error."},
#             status=status.HTTP_500_INTERNAL_SERVER_ERROR
#         )


@api_view(["GET"])
@auth_free
def get_items_by_community(request):
    try:
        community_id = request.query_params.get("community_id")
        if not community_id:
            return Response(
                {"success": False, "message": "Missing 'community_id' in query parameters."},
                status=status.HTTP_400_BAD_REQUEST
            )

        # Optional filters
        item_type = request.query_params.get("item_type")
        item_state = request.query_params.get("item_state")

        # Pagination parameters
        limit = int(request.query_params.get("limit", 5))
        offset = int(request.query_params.get("offset", 0))

        # Base queryset
        items_qs = Item.objects.filter(community_id=community_id)

        if item_type:
            items_qs = items_qs.filter(item_type=item_type)

        if item_state:
            items_qs = items_qs.filter(state=item_state)

        items_qs = items_qs.order_by('-created_at')
        total_count = items_qs.count()
        paginated_items = items_qs[offset:offset + limit]

        data = []
        for item in paginated_items:
            data.append({
                'id': item.id,
                'number': item.user.contact_number,
                'title': item.title,
                'item_type': item.item_type,
                'state': item.state,
                'created_at': item.created_at.strftime('%Y-%m-%d %H:%M:%S'),
            })

        return Response({
            "success": True,
            "data": data,
            "total": total_count,
            "limit": limit,
            "offset": offset,
            "has_more": offset + limit < total_count
        }, status=status.HTTP_200_OK)

    except Exception as e:
        print("Exception in get_items_by_community API:", str(e))
        return Response(
            {"success": False, "message": "Internal server error."},
            status=status.HTTP_500_INTERNAL_SERVER_ERROR
        )

