from rest_framework.decorators import api_view, schema
from rest_framework.response import Response
from rest_framework import status
from .utils import *
from .mws_indicators import get_generate_filter_mws_data, download_KYL_filter_data
from .village_indicators import get_generate_filter_data_village
from utilities.auth_utils import auth_free
import logging
from utilities.gee_utils import (
    valid_gee_text,
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

@api_view(["GET"])
@auth_free
@schema(None)
def generate_excel_file_layer(request):
    try:
        state = valid_gee_text(request.query_params.get("state", "").lower())
        district = valid_gee_text(request.query_params.get("district", "").lower())
        block = valid_gee_text(request.query_params.get("block", "").lower())

        logging.info(f"Request to generate Excel for state: {state}, district: {district}, block: {block}")
        
        base_path = os.path.join(EXCEL_PATH, 'data/stats_excel_files')
        state_path = os.path.join(base_path, state.upper())
        district_path = os.path.join(state_path, district.upper())
        filename = f"{district}_{block}.xlsx"
        file_path = os.path.join(district_path, filename)

        # If file exists, return it directly
        if os.path.exists(file_path):
            logging.info(f"Excel file already exists at: {file_path}")
        else:
            logging.info("Excel file does not exist. Generating...")
            if not get_vector_layer_geoserver(state, district, block):
                raise ValueError("Failed to generate vector layer from GeoServer.")
            
            os.makedirs(district_path, exist_ok=True)
            
            excel_file_path = download_layers_excel_file(state, district, block)
            logging.info(f"Excel file generated at: {excel_file_path}")
            
            if not excel_file_path or not os.path.exists(excel_file_path):
                raise ValueError("Failed to download or locate generated Excel file.")
            
            file_path = excel_file_path  # Use the actual generated path, in case it's different

        # Serve the file
        with open(file_path, 'rb') as file:
            response = HttpResponse(
                file.read(),
                content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
            response['Content-Disposition'] = f'attachment; filename={filename}'
            return response

    except Exception as e:
        logging.error(f"Error generating Excel file: {str(e)}")
        return Response({
            "status": "error",
            "message": str(e)
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)




@api_view(["GET"])
@auth_free
@schema(None)
def generate_kyl_data_excel(request):
    try:
        print("Inside generate_kyl_data_excel API.")
        
        state = valid_gee_text(request.query_params.get("state", "").lower())
        district = valid_gee_text(request.query_params.get("district", "").lower())
        block = valid_gee_text(request.query_params.get("block", "").lower())
        file_type = request.query_params.get("file_type", "").lower().strip()
        
        # Generate data for the file
        creating_kyl_data = get_generate_filter_mws_data(state, district, block, file_type)
        print("Data generated in the file")
        excel_file = download_KYL_filter_data(state, district, block, file_type)
        logging.info(f"Download function returned: {excel_file}")
        if excel_file:
            if isinstance(excel_file, str) and os.path.exists(excel_file):
                with open(excel_file, 'rb') as file:
                    response = HttpResponse(file.read(), content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
                    response['Content-Disposition'] = f'attachment; filename={district}_{block}_KYL_filter_data.{file_type}'
                    return response
            else:
                raise ValueError("Invalid file format received from download_KYL_filter_data.")
        else:
            raise ValueError("Failed to download the KYL filter data file")
        
    except Exception as e:
        logging.error(f"Validation error: {str(e)}")
        return Response({
            "status": "error",
            "message": str(e)
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(["GET"])
@auth_free
@schema(None)
def generate_kyl_village_data(request):
    try:
        print("Inside generate_filter_data_village API.")
        
        state = valid_gee_text(request.query_params.get("state", "").lower())
        district = valid_gee_text(request.query_params.get("district", "").lower())
        block = valid_gee_text(request.query_params.get("block", "").lower())
        village_kyl_json =  get_generate_filter_data_village(state, district, block)
        if village_kyl_json:
            if isinstance(village_kyl_json, str) and os.path.exists(village_kyl_json):
                with open(village_kyl_json, 'rb') as file:
                    response = HttpResponse(file.read(), content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
                    response['Content-Disposition'] = f'attachment; filename={district}_{block}_KYL_village_data.json'
                    return response
            else:
                raise ValueError("Invalid file format received from download_KYL_filter_data.")
        else:
            raise ValueError("Failed to download the KYL filter data file")

    except Exception as e:
        logging.error(f"Validation error: {str(e)}")
        return Response({
            "status": "error",
            "message": str(e)
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


