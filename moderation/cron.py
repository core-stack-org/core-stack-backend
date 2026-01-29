from .views import sync_odk_to_csdb


def run_daily_odk_job():
    print("Populating CSDB with ODK data with cron job...")
    sync_odk_to_csdb()
