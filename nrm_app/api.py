from rest_framework.decorators import api_view
from rest_framework.response import Response
from rest_framework import status
from rest_framework.views import APIView
import json
import logging

logger = logging.getLogger(__name__)


def load_json_data():
    with open("data/output/all_states.json", "r") as f:
        return json.load(f)


# api for adding state,
class GetStatesAPI(APIView):
    def get(self, request):
        json_data = load_json_data()

        states = [state["name"] for state in json_data["states"]]
        return Response(states)


class GetDistrictsAPI(APIView):
    def get(self, request, state_name):
        json_data = load_json_data()

        for state in json_data["states"]:
            if state["name"].lower() == state_name.lower():
                districts = [district["name"] for district in state["districts"]]
                return Response(districts)
        return Response({"error": "State not found"}, status=404)


class GetBlocksAPI(APIView):
    def get(self, request, state_name, district_name):
        json_data = load_json_data()

        for state in json_data["states"]:
            if state["name"].lower() == state_name.lower():
                for district in state["districts"]:
                    if district["name"].lower() == district_name.lower():
                        blocks = [block["name"] for block in district["blocks"]]
                        return Response(blocks)
        return Response({"error": "District not found"}, status=404)
