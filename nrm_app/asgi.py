"""
ASGI config for nrm_app project.

It exposes the ASGI callable as a module-level variable named ``application``.

For more information on this file, see
https://docs.djangoproject.com/en/4.2/howto/deployment/asgi/
"""

import os

from django.core.asgi import get_asgi_application

from nrm_app.runtime import configure_runtime_environment

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "nrm_app.settings")
configure_runtime_environment()

application = get_asgi_application()
