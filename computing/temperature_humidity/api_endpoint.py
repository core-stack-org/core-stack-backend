"""
API endpoint for Temperature and Humidity layer generation

This file contains the endpoint code to be added to computing/api.py
"""

# Add this import at the top of api.py:
# from computing.temperature_humidity.temperature_humidity import generate_temperature_humidity

# Add this endpoint function to api.py:

@api_view(["POST"])
@schema(None)
def generate_climate_layer(request):
    """
    API endpoint to generate temperature and humidity layers at 5km resolution

    Request Body:
    {
        "state": "Andhra Pradesh",
        "district": "Ananthapur",  // optional
        "block": "Nallacheruvu",  // optional
        "temporal_range": "monthly",  // "monthly", "seasonal", or "annual"
        "start_date": "2024-01-01",  // optional, format: YYYY-MM-DD
        "end_date": "2024-12-31",  // optional, format: YYYY-MM-DD
        "gee_account_id": 1,  // optional, defaults to settings.GEE_ACCOUNT_ID
        "helper_account_id": 2  // optional, defaults to settings.GEE_HELPER_ACCOUNT_ID
    }

    Returns:
    {
        "message": "Temperature and humidity layer generation started",
        "task_id": "celery-task-id",
        "state": "Andhra Pradesh",
        "district": "Ananthapur",
        "block": "Nallacheruvu"
    }
    """
    print("Inside generate_climate_layer")
    try:
        # Extract parameters from request
        state = request.data.get("state")
        district = request.data.get("district")
        block = request.data.get("block")
        temporal_range = request.data.get("temporal_range", "monthly")
        start_date = request.data.get("start_date")
        end_date = request.data.get("end_date")
        gee_account_id = request.data.get("gee_account_id", settings.GEE_ACCOUNT_ID)
        helper_account_id = request.data.get("helper_account_id", settings.GEE_HELPER_ACCOUNT_ID)

        # Validate required parameters
        if not state:
            return Response(
                {"error": "State is required"},
                status=status.HTTP_400_BAD_REQUEST
            )

        # Validate temporal range
        valid_ranges = ["monthly", "seasonal", "annual"]
        if temporal_range not in valid_ranges:
            return Response(
                {"error": f"Invalid temporal_range. Must be one of: {valid_ranges}"},
                status=status.HTTP_400_BAD_REQUEST
            )

        # Validate date format if provided
        if start_date:
            try:
                datetime.strptime(start_date, '%Y-%m-%d')
            except ValueError:
                return Response(
                    {"error": "Invalid start_date format. Use YYYY-MM-DD"},
                    status=status.HTTP_400_BAD_REQUEST
                )

        if end_date:
            try:
                datetime.strptime(end_date, '%Y-%m-%d')
            except ValueError:
                return Response(
                    {"error": "Invalid end_date format. Use YYYY-MM-DD"},
                    status=status.HTTP_400_BAD_REQUEST
                )

        # Launch async task
        task = generate_temperature_humidity.apply_async(
            kwargs={
                "state": state,
                "district": district,
                "block": block,
                "temporal_range": temporal_range,
                "start_date": start_date,
                "end_date": end_date,
                "gee_account_id": gee_account_id,
                "helper_account_id": helper_account_id,
            },
            queue="nrm",
        )

        return Response(
            {
                "message": "Temperature and humidity layer generation started",
                "task_id": task.id,
                "state": state,
                "district": district,
                "block": block,
                "temporal_range": temporal_range,
                "date_range": {
                    "start_date": start_date,
                    "end_date": end_date
                }
            },
            status=status.HTTP_200_OK,
        )

    except Exception as e:
        print("Exception in generate_climate_layer api :: ", e)
        return Response(
            {"error": str(e)},
            status=status.HTTP_500_INTERNAL_SERVER_ERROR
        )


# Also add this task status endpoint:

@api_view(["GET"])
@schema(None)
def get_climate_task_status(request, task_id):
    """
    Get the status of a climate layer generation task

    Returns:
    {
        "task_id": "celery-task-id",
        "state": "PENDING|PROGRESS|SUCCESS|FAILURE",
        "result": {...},  // if successful
        "error": "...",  // if failed
        "progress": {
            "current": 50,
            "total": 100,
            "status": "Processing temperature data..."
        }
    }
    """
    try:
        from celery.result import AsyncResult
        task = AsyncResult(task_id)

        response = {
            "task_id": task_id,
            "state": task.state,
        }

        if task.state == 'PENDING':
            response['status'] = 'Task is waiting to be processed'
        elif task.state == 'PROGRESS':
            response['progress'] = task.info
        elif task.state == 'SUCCESS':
            response['result'] = task.result
        elif task.state == 'FAILURE':
            response['error'] = str(task.info)
        else:
            response['status'] = task.state

        return Response(response, status=status.HTTP_200_OK)

    except Exception as e:
        print("Exception in get_climate_task_status :: ", e)
        return Response(
            {"error": str(e)},
            status=status.HTTP_500_INTERNAL_SERVER_ERROR
        )


# Add these URL patterns to computing/urls.py:
"""
from django.urls import path
from . import api

urlpatterns = [
    # ... existing patterns ...

    # Temperature and Humidity endpoints
    path('generate-climate-layer/', api.generate_climate_layer, name='generate-climate-layer'),
    path('climate-task-status/<str:task_id>/', api.get_climate_task_status, name='climate-task-status'),
]
"""