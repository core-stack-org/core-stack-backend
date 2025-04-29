"""
WSGI config for nrm_app project.

It exposes the WSGI callable as a module-level variable named ``application``.

For more information on this file, see
https://docs.djangoproject.com/en/4.2/howto/deployment/wsgi/
"""

import os

from django.core.wsgi import get_wsgi_application

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "nrm_app.settings")

os.environ['GDAL_DATA'] = '/home/ubuntu/prod_dir/nrm-app/venv/envs/corestack/share/gdal'
os.environ['LD_LIBRARY_PATH'] = '/home/ubuntu/prod_dir/nrm-app/venv/envs/corestack/lib'

application = get_wsgi_application()
