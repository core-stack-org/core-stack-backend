"""
Celery task for Temperature and Humidity layer generation at 5km resolution
"""
import json
import traceback
from celery import current_task
from django.conf import settings
from nrm_app.celery import app
from utilities.gee_utils import ee_initialize
from utilities.constants import GEE_PATHS
from computing.models import Dataset, Layer
from geoadmin.models import StateSOI, DistrictSOI, TehsilSOI
from .generate_temp_humidity_layers import (
    generate_temperature_layer,
    generate_humidity_layer,
    generate_climate_vectors
)


@app.task(bind=True)
def generate_temperature_humidity(
    self,
    state,
    district=None,
    block=None,
    temporal_range="monthly",
    start_date=None,
    end_date=None,
    gee_account_id=settings.GEE_ACCOUNT_ID,
    helper_account_id=settings.GEE_HELPER_ACCOUNT_ID
):
    """
    Generate temperature and humidity layers at 5km resolution

    Args:
        state: State name
        district: District name (optional)
        block: Block/Tehsil name (optional)
        temporal_range: "monthly", "seasonal", or "annual"
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
        gee_account_id: Main GEE account ID
        helper_account_id: Helper GEE account ID for parallel processing

    Returns:
        dict: Status and layer information
    """
    try:
        # Update task state
        current_task.update_state(
            state='PROGRESS',
            meta={'current': 10, 'total': 100, 'status': 'Initializing...'}
        )

        # Initialize GEE with main account
        ee_initialize(gee_account_id)

        # Validate inputs
        state_obj = StateSOI.objects.filter(name__iexact=state).first()
        if not state_obj:
            raise ValueError(f"State {state} not found")

        district_obj = None
        if district:
            district_obj = DistrictSOI.objects.filter(
                name__iexact=district,
                state=state_obj
            ).first()
            if not district_obj:
                raise ValueError(f"District {district} not found in {state}")

        block_obj = None
        if block and district_obj:
            block_obj = TehsilSOI.objects.filter(
                name__iexact=block,
                district=district_obj
            ).first()
            if not block_obj:
                raise ValueError(f"Block {block} not found in {district}")

        # Get or create dataset
        dataset, _ = Dataset.objects.get_or_create(
            name="Temperature and Humidity",
            defaults={
                'data_type': 'raster',
                'workspace': 'climate',
                'style': 'temperature_gradient',
                'description': 'Temperature and Humidity layers at 5km resolution'
            }
        )

        # Update progress
        current_task.update_state(
            state='PROGRESS',
            meta={'current': 20, 'total': 100, 'status': 'Fetching temperature data...'}
        )

        # Generate temperature layer
        temp_result = generate_temperature_layer(
            state_obj=state_obj,
            district_obj=district_obj,
            block_obj=block_obj,
            start_date=start_date,
            end_date=end_date,
            temporal_range=temporal_range,
            gee_account_id=gee_account_id
        )

        # Update progress
        current_task.update_state(
            state='PROGRESS',
            meta={'current': 40, 'total': 100, 'status': 'Fetching humidity data...'}
        )

        # Generate humidity layer using helper account for parallel processing
        humidity_result = generate_humidity_layer(
            state_obj=state_obj,
            district_obj=district_obj,
            block_obj=block_obj,
            start_date=start_date,
            end_date=end_date,
            temporal_range=temporal_range,
            gee_account_id=helper_account_id
        )

        # Update progress
        current_task.update_state(
            state='PROGRESS',
            meta={'current': 60, 'total': 100, 'status': 'Generating vector polygons...'}
        )

        # Generate vector polygons with both temperature and humidity attributes
        vector_result = generate_climate_vectors(
            temperature_asset=temp_result['asset_path'],
            humidity_asset=humidity_result['asset_path'],
            state_obj=state_obj,
            district_obj=district_obj,
            block_obj=block_obj,
            gee_account_id=gee_account_id
        )

        # Update progress
        current_task.update_state(
            state='PROGRESS',
            meta={'current': 80, 'total': 100, 'status': 'Saving layer information...'}
        )

        # Create layer records
        layers_created = []

        # Temperature layer
        temp_layer = Layer.objects.create(
            dataset=dataset,
            state=state_obj,
            district=district_obj,
            block=block_obj,
            name=f"Temperature_5km_{state}_{district or 'all'}_{block or 'all'}",
            gee_asset_path=temp_result['asset_path'],
            metadata={
                'type': 'temperature',
                'resolution': '5km',
                'units': 'degrees_celsius',
                'temporal_range': temporal_range,
                'start_date': start_date,
                'end_date': end_date,
                'statistics': temp_result.get('statistics', {})
            }
        )
        layers_created.append(temp_layer.id)

        # Humidity layer
        humidity_layer = Layer.objects.create(
            dataset=dataset,
            state=state_obj,
            district=district_obj,
            block=block_obj,
            name=f"Humidity_5km_{state}_{district or 'all'}_{block or 'all'}",
            gee_asset_path=humidity_result['asset_path'],
            metadata={
                'type': 'humidity',
                'resolution': '5km',
                'units': 'percentage',
                'temporal_range': temporal_range,
                'start_date': start_date,
                'end_date': end_date,
                'statistics': humidity_result.get('statistics', {})
            }
        )
        layers_created.append(humidity_layer.id)

        # Vector layer
        vector_layer = Layer.objects.create(
            dataset=dataset,
            state=state_obj,
            district=district_obj,
            block=block_obj,
            name=f"Climate_Vectors_5km_{state}_{district or 'all'}_{block or 'all'}",
            gee_asset_path=vector_result['asset_path'],
            metadata={
                'type': 'vector',
                'resolution': '5km',
                'attributes': ['mean_temperature', 'mean_humidity', 'area_km2'],
                'temporal_range': temporal_range,
                'start_date': start_date,
                'end_date': end_date,
                'polygon_count': vector_result.get('polygon_count', 0)
            }
        )
        layers_created.append(vector_layer.id)

        # Update progress
        current_task.update_state(
            state='PROGRESS',
            meta={'current': 100, 'total': 100, 'status': 'Complete!'}
        )

        return {
            'status': 'success',
            'message': 'Temperature and humidity layers generated successfully',
            'layers': layers_created,
            'temperature': {
                'asset_path': temp_result['asset_path'],
                'statistics': temp_result.get('statistics', {})
            },
            'humidity': {
                'asset_path': humidity_result['asset_path'],
                'statistics': humidity_result.get('statistics', {})
            },
            'vectors': {
                'asset_path': vector_result['asset_path'],
                'polygon_count': vector_result.get('polygon_count', 0)
            }
        }

    except Exception as e:
        traceback.print_exc()
        current_task.update_state(
            state='FAILURE',
            meta={'error': str(e)}
        )

        return {
            'status': 'error',
            'message': str(e),
            'traceback': traceback.format_exc()
        }