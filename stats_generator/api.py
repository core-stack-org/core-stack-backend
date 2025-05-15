from rest_framework.decorators import api_view
from rest_framework.response import Response
from rest_framework import status
from .utils import *
from .mws_indicators import get_generate_filter_mws_data, download_KYL_filter_data
from .village_indicators import get_generate_filter_data_village
from utilities.auth_utils import auth_free
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

@api_view(["GET"])
@auth_free
def generate_excel_file_layer(request):
    try:
        state = request.query_params.get("state", "").lower().strip()
        district = request.query_params.get("district", "").lower().strip().replace(" ", "_")
        block = request.query_params.get("block", "").lower().strip().replace(" ", "_")
        logging.info(f"Generating Excel file for state: {state}, district: {district}, block: {block}")
        creating_xlsx = get_vector_layer_geoserver(state, district, block)
        
        if creating_xlsx:
            excel_file = download_layers_excel_file(state, district, block)
            logging.info(f"Download function returned: {excel_file}")
            if excel_file:
                if isinstance(excel_file, str) and os.path.exists(excel_file):
                    with open(excel_file, 'rb') as file:
                        response = HttpResponse(file.read(), content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
                        response['Content-Disposition'] = f'attachment; filename={district}_{block}.xlsx'
                        return response
                
                else:
                    raise ValueError("Invalid file format received from download_layers_excel_file.")
            else:
                raise ValueError("Failed to download Excel file")
        else:
            raise ValueError("Failed to generate Excel file")
            
    except Exception as e:
        logging.error(f"Validation error: {str(e)}")
        return Response({
            "status": "error",
            "message": str(e)
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(["GET"])
@auth_free
def generate_kyl_data_excel(request):
    try:
        print("Inside generate_kyl_data_excel API.")
        
        state = request.query_params.get("state", "").lower().strip()
        district = request.query_params.get("district", "").lower().strip().replace(" ", "_")
        block = request.query_params.get("block", "").lower().strip().replace(" ", "_")
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
def generate_kyl_village_data(request):
    try:
        print("Inside generate_filter_data_village API.")
        
        state = request.query_params.get("state").lower()
        district = request.query_params.get("district").lower().replace(" ", "_")
        block = request.query_params.get("block").lower().replace(" ", "_")
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


