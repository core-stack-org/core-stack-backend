from django.http import HttpRequest
from rest_framework import status
from rest_framework.decorators import api_view
from rest_framework.request import Request
from rest_framework.response import Response

from .models import Block, District, State
from .serializers import BlockSerializer, DistrictSerializer, StateSerializer
from .utils import transform_data, activated_entities
from utilities.auth_utils import auth_free


# state id is the census code while the district id is the id of the district from the DB
# block id is the id of the block from the DB
@api_view(["GET"])
@auth_free
def get_states(request):
    try:
        states = State.objects.all()
        serializer = StateSerializer(states, many=True)
        return Response({"states": serializer.data}, status=status.HTTP_200_OK)
    except Exception as e:
        print("Exception in get_states api :: ", e)
        return Response({"Exception": e}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(["GET"])
@auth_free
def get_districts(request, state_id):
    try:
        districts = District.objects.filter(state_id=state_id)
        serializer = DistrictSerializer(districts, many=True)
        return Response({"districts": serializer.data}, status=status.HTTP_200_OK)
    except Exception as e:
        print("Exception in get_districts api :: ", e)
        return Response({"Exception": e}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(["GET"])
@auth_free
def get_blocks(request, district_id):
    try:
        blocks = Block.objects.filter(district=district_id)
        serializer = BlockSerializer(blocks, many=True)
        return Response({"blocks": serializer.data}, status=status.HTTP_200_OK)
    except Exception as e:
        print("Exception in get_blocks api :: ", e)
        return Response({"Exception": e}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(["POST"])
@auth_free
def activate_entities(request):
    try:
        messages = []

        state_id = request.data.get("state_id")  # census code of the state
        district_id = request.data.get("district_id")  # id of the district
        block_id = request.data.get("block_id")  # id of the block

        state = None
        if state_id:
            state = State.objects.get(state_census_code=state_id)

        if district_id:
            district = District.objects.get(id=district_id)
            district.active_status = not district.active_status
            district.save()
            status_msg = "activated" if district.active_status else "deactivated"
            messages.append(
                f"District '{district.district_name}' has been {status_msg}"
            )

        if block_id:
            block = Block.objects.get(id=block_id)
            block.active_status = not block.active_status
            block.save()
            status_msg = "activated" if block.active_status else "deactivated"
            messages.append(f"Block '{block.block_name}' has been {status_msg}")

        if state:
            any_district_active = District.objects.filter(
                state=state, active_status=True
            ).exists()
            any_block_active = Block.objects.filter(
                district__state=state, active_status=True
            ).exists()
            state.active_status = any_district_active or any_block_active
            state.save()
            status_msg = "activated" if state.active_status else "deactivated"
            messages.insert(
                0,
                f"State '{state.state_name}' has been {status_msg} based on its districts/blocks status",
            )

        status_code = (
            status.HTTP_200_OK
            if any(
                entity.active_status for entity in [district, block, state] if entity
            )
            else status.HTTP_204_NO_CONTENT
        )
        return Response({"message": ", ".join(messages)}, status=status_code)

    except Exception as e:
        print(f"Exception in activate_entities api: {e}")
        return Response(
            {"Exception": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR
        )


@api_view(["GET"])
@auth_free
def proposed_blocks(request):
    try:
        response_data = activated_entities()
        transformed_data = transform_data(data=response_data)
        return Response(transformed_data, status=status.HTTP_200_OK)
    except Exception as e:
        print("Exception in proposed_blocks api :: ", e)
        return Response(
            {"Exception": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR
        )


@api_view(["PATCH"])
@auth_free
def activate_location(request):
    """
    Update activation status of a location (state/district/block).

    Request body should contain:
    {
        "location_type": "state|district|block",
        "location_id": str,
        "active": bool
    }
    
    Hierarchical validation rules:
    - Districts can only be activated if their State is active
    - Blocks can only be activated if both their State and District are active
    """
    try:
        location_type = request.data.get("location_type")
        location_id = request.data.get("location_id")
        active = request.data.get("active")

        if not all([location_type, location_id, active is not None]):
            return Response(
                {
                    "error": "Missing required fields. Please provide location_type, location_id, and active status"
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        if location_type not in ["state", "district", "block"]:
            return Response(
                {
                    "error": "Invalid location_type. Must be one of: state, district, block"
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        try:
            if location_type == "state":
                location = State.objects.get(state_census_code=location_id)
            elif location_type == "district":
                location = District.objects.get(id=location_id)
                
                if active and not location.state.active_status:
                    return Response(
                        {"error": "State not active yet, please activate."},
                        status=status.HTTP_400_BAD_REQUEST,
                    )
            else:  
                location = Block.objects.get(id=location_id)
                
                if active:
                    state_active = location.district.state.active_status
                    district_active = location.district.active_status
                    
                    if not state_active and not district_active:
                        return Response(
                            {"error": "State and District not active yet, please activate them first."},
                            status=status.HTTP_400_BAD_REQUEST,
                        )
                    elif not district_active:
                        return Response(
                            {"error": "District not active yet, please activate."},
                            status=status.HTTP_400_BAD_REQUEST,
                        )
                    elif not state_active:
                        return Response(
                            {"error": "State not active yet, please activate."},
                            status=status.HTTP_400_BAD_REQUEST,
                        )

            if location.active_status != active:
                location.active_status = active
                location.save()
                message = "Successfully activated a location" if active else "Successfully deactivated a location"
                return Response(
                    {
                        "message": message,
                        "location_type": location_type,
                        "location_id": location_id,
                        "active": active,
                    },
                    status=status.HTTP_200_OK,
                )
            else:
                message = "Location already active" if active else "Location already inactive"
                return Response(
                    {
                        "message": message,
                        "location_type": location_type,
                        "location_id": location_id,
                        "active": active,
                    },
                    status=status.HTTP_200_OK,
                )

        except (State.DoesNotExist, District.DoesNotExist, Block.DoesNotExist):
            return Response(
                {"error": f"{location_type.title()} with id {location_id} not found"},
                status=status.HTTP_404_NOT_FOUND,
            )

    except Exception as e:
        print(f"Exception in activate_location api: {e}")
        return Response({"error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
