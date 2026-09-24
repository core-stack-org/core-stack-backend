"""
WSGI config for nrm_app project.

It exposes the WSGI callable as a module-level variable named ``application``.

For more information on this file, see
https://docs.djangoproject.com/en/4.2/howto/deployment/wsgi/
"""

import os
import site
from pathlib import Path
import environ
from django.core.wsgi import get_wsgi_application

env = environ.Env()
ENV_FILE = Path(__file__).resolve().parent / ".env"
environ.Env.read_env(str(ENV_FILE))

DEBUG = env.bool("DEBUG", default=False)

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "nrm_app.settings")

conda_env = os.path.dirname(site.__file__).split("/lib")[0]
gdal_data = os.path.join(conda_env, "share", "gdal")
proj_lib = os.path.join(conda_env, "share", "proj")
lib_path = os.path.join(conda_env, "lib")

if os.path.isdir(gdal_data):
    os.environ["GDAL_DATA"] = gdal_data
if os.path.isdir(proj_lib):
    os.environ["PROJ_LIB"] = proj_lib
    os.environ["PROJ_DATA"] = proj_lib
if os.path.isdir(lib_path):
    os.environ["LD_LIBRARY_PATH"] = lib_path

if DEBUG:
    print("CONDA ENV: ", conda_env)

application = get_wsgi_application()
