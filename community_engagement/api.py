from django.http import HttpRequest
from rest_framework import status
from rest_framework.decorators import api_view
from rest_framework.request import Request
from rest_framework.response import Response
import boto3
import uuid
import pandas as pd

from .models import Community, Item, Community_user_mapping, Media
from .utils import get_media_type, get_community_summary_data
from geoadmin.models import State
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

        return Response({"success": True}, status=status.HTTP_201_CREATED)
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
        state_id = request.data.get("state_id", "")
        district_id = request.data.get("district_id", "")
        block_id = request.data.get("block_id", "")

        if block_id:
            communities = Community.objects.filter(project__block_id=block_id)
            if not communities.exist():
                if district_id:
                    communities = Community.objects.filter(project__district_id=district_id)
                    if not communities.exist():
                        if state_id:
                            communities = Community.objects.filter(project__state_id=state_id)
                        else:
                            communities = Community.objects.all()
                else:
                    if state_id:
                        communities = Community.objects.filter(project__state_id=state_id)
                    else:
                        communities = Community.objects.all()
        else:
            if district_id:
                communities = Community.objects.filter(project__district_id=district_id)
                if not communities.exist():
                    if state_id:
                        communities = Community.objects.filter(project__state_id=state_id)
                    else:
                        communities = Community.objects.all()
            else:
                if state_id:
                    communities = Community.objects.filter(project__state_id=state_id)
                else:
                    communities = Community.objects.all()

        data = []
        for community in communities:
            data.append(get_community_summary_data(community.id))

        return Response({"success": True, "data": data}, status=status.HTTP_201_CREATED)
    except Exception as e:
        print("Exception in get_communities_by_location api :: ", e)
        return Response({"success": False}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(["GET"])
@auth_free
def get_community_by_user(request):
    try:
        data = []
        number = request.data.get('number')
        users = User.objects.filter(contact_number=number)
        if users.exists():
            user = users[0]
            communities = Community_user_mapping.objects.filter(user=user)
            for community in communities:
                data.append(get_community_summary_data(community.id))

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
        data = {}
        number = request.data.get("number")
        if number:
            user_objs = User.objects.filter(contact_number=number)
            if user_objs.exists():
                user = user_objs[0]
                community_user_mapping_qs = Community_user_mapping.objects.filter(user=user)
                if community_user_mapping_qs.exists():
                    communities_list = []
                    for community_user_mapping_obj in community_user_mapping_qs:
                        communities_list.append(get_community_summary_data(community_user_mapping_obj.community.id))

                    data["is_in_community"] = True
                    data["data_type"] = "community"
                    data["data"] = communities_list
                else:
                    states = State.objects.all()
                    states_list = []
                    for state in states:
                        state_dict = {"id": state.pk, "name": state.state_name}
                        states_list.append(state_dict)

                    data["is_in_community"] = False
                    data["data_type"] = "state"
                    data["data"] = states_list
        return Response({"success": True, "data": data}, status=status.HTTP_201_CREATED)
    except Exception as e:
        print("Exception in if_user_in_community api :: ", e)
        return Response({"success": False}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)