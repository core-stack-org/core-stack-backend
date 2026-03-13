from nrm_app.celery import app
from utilities.logger import setup_logger
from moderation.views import sync_odk_to_csdb

logger = setup_logger(__name__)


@app.task(bind=True, name="moderation.sync_odk_data_task")
def sync_odk_data_task(self):
    logger.info("Starting scheduled ODK data sync")
    try:
        result = sync_odk_to_csdb()
        logger.info("ODK data sync completed successfully")
        return {"status": "success", "result": str(result)}
    except Exception as e:
        logger.error(f"ODK data sync failed: {e}")
        raise
