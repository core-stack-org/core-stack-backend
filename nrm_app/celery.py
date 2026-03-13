import os
from celery import Celery
from nrm_app.settings import INSTALLED_APPS

# set the default Django settings module for the 'celery' program.
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "nrm_app.settings")

app = Celery("nrm_app")

# Using a string here means the worker doesn't
# have to serialize the configuration object to
# child processes. - namespace='CELERY' means all
# celery-related configuration keys should
# have a `CELERY_` prefix.
app.config_from_object("django.conf:settings", namespace="CELERY")

# Load task modules from all registered Django app configs.
app.autodiscover_tasks(INSTALLED_APPS)
