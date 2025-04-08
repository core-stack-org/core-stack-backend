from rest_framework import status
from rest_framework.decorators import api_view
from rest_framework.response import Response

from .gen_dpr import create_dpr_document, get_plan, send_dpr_email
from .utils import validate_email
from utilities.logger import setup_logger
from utilities.auth_utils import auth_free


logger = setup_logger(__name__)


@api_view(["POST"])
@auth_free
def generate_dpr(request):
    try:
        plan_id = request.data.get("plan_id")
        email_id = request.data.get("email_id")

        logger.info(
            "Generating DPR for plan ID: %s and email ID: %s", plan_id, email_id
        )

        valid_email = validate_email(email_id)

        if not valid_email:
            return Response(
                {"error": "Invalid email address"}, status=status.HTTP_400_BAD_REQUEST
            )

        plan = get_plan(plan_id)
        logger.info("Plan found: %s", plan)
        if plan is None:
            return Response(
                {"error": "Plan not found"}, status=status.HTTP_404_NOT_FOUND
            )

        doc = create_dpr_document(plan)
        send_dpr_email(doc, email_id, plan.plan)

        return Response(
            {
                "message": f"DPR generated successfully and sent to the email ID: {email_id}"
            },
            status=status.HTTP_201_CREATED,
        )

    except Exception as e:
        logger.exception("Exception in generate_dpr api :: ", e)
        return Response({"Exception": e}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
