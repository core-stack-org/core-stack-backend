"""
Django settings for nrm_app project.

Generated by 'django-admin startproject' using Django 4.2.2.

For more information on this file, see
https://docs.djangoproject.com/en/4.2/topics/settings/

For the full list of settings and their values, see
https://docs.djangoproject.com/en/4.2/ref/settings/
"""

import os
from pathlib import Path
import environ

env = environ.Env()
# reading .env file
environ.Env.read_env()


# Build paths inside the project like this: BASE_DIR / 'subdir'.
BASE_DIR = Path(__file__).resolve().parent.parent


# Quick-start development settings - unsuitable for production
# See https://docs.djangoproject.com/en/4.2/howto/deployment/checklist/

# SECURITY WARNING: keep the secret key used in production secret!
SECRET_KEY = env("SECRET_KEY")


# SECURITY WARNING: don't run with debug turned on in production!
DEBUG = env("DEBUG")

ODK_USERNAME = env("ODK_USERNAME")
ODK_PASSWORD = env("ODK_PASSWORD")

DB_NAME = env("DB_NAME")
DB_USER = env("DB_USER")
DB_PASSWORD = env("DB_PASSWORD")

USERNAME_GESDISC = env("USERNAME_GESDISC")
PASSWORD_GESDISC = env("PASSWORD_GESDISC")
STATIC_ROOT = "static/"
ALLOWED_HOSTS = [
    "geoserver.core-stack.org",
    "127.0.0.1",
    "localhost",
    "0.0.0.0",
    "e697-2001-df4-e000-3fc4-e2e1-373c-2498-b87c.ngrok-free.app",
]

# Application definition

INSTALLED_APPS = [
    "django.contrib.admin",
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.sessions",
    "django.contrib.messages",
    "django.contrib.staticfiles",
    # django apps
    "app_controller",
    "gee_computing",
    "plans",
    "computing",
    "dpr",
    "geoadmin",
    # third party apps
    "rest_framework",
    "corsheaders",
    "drf_yasg",
    "stats_generator",
]

CORS_ALLOW_ALL_ORIGINS = True

CORS_ALLOWED_ORIGINS = [
    "http://gramvaanimoderationtest.s3-website.ap-south-1.amazonaws.com",
    "http://127.0.0.1:8000",
    "http://192.168.222.27:8000",
    "http://192.168.222.23:3000",
    "http://192.168.20.236:3000",
]

CORS_ALLOW_HEADERS = [
    "ngrok-skip-browser-warning",
    "content-type",
]

MIDDLEWARE = [
    "django.middleware.security.SecurityMiddleware",
    "django.contrib.sessions.middleware.SessionMiddleware",
    "corsheaders.middleware.CorsMiddleware",
    "django.middleware.common.CommonMiddleware",
    "django.middleware.csrf.CsrfViewMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",
    "django.contrib.messages.middleware.MessageMiddleware",
    "django.middleware.clickjacking.XFrameOptionsMiddleware",
]

ROOT_URLCONF = "nrm_app.urls"

TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "DIRS": [os.path.join(BASE_DIR, "templates")],
        "APP_DIRS": True,
        "OPTIONS": {
            "context_processors": [
                "django.template.context_processors.debug",
                "django.template.context_processors.request",
                "django.contrib.auth.context_processors.auth",
                "django.contrib.messages.context_processors.messages",
            ],
        },
    },
]

WSGI_APPLICATION = "nrm_app.wsgi.application"


# Database
# https://docs.djangoproject.com/en/4.2/ref/settings/#databases

DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.mysql",
        "NAME": DB_NAME,
        "USER": DB_USER,
        "PASSWORD": DB_PASSWORD,
        "HOST": "127.0.0.1",
        "PORT": "",
        "OPTIONS": {
            "unix_socket": "/tmp/mysql.sock",
        },
    }
}


# Password validation
# https://docs.djangoproject.com/en/4.2/ref/settings/#auth-password-validators

AUTH_PASSWORD_VALIDATORS = [
    {
        "NAME": "django.contrib.auth.password_validation.UserAttributeSimilarityValidator",
    },
    {
        "NAME": "django.contrib.auth.password_validation.MinimumLengthValidator",
    },
    {
        "NAME": "django.contrib.auth.password_validation.CommonPasswordValidator",
    },
    {
        "NAME": "django.contrib.auth.password_validation.NumericPasswordValidator",
    },
]


# Internationalization
# https://docs.djangoproject.com/en/4.2/topics/i18n/

LANGUAGE_CODE = "en-us"

TIME_ZONE = "Asia/Kolkata"

USE_I18N = True

USE_TZ = True


# Static files (CSS, JavaScript, Images)
# https://docs.djangoproject.com/en/4.2/howto/static-files/

STATIC_URL = "static/"
ASSET_DIR = "/home/ubuntu/cfpt/core-stack-backend/assets/"

# Default primary key field type
# https://docs.djangoproject.com/en/4.2/ref/settings/#default-auto-field

DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"

# ODK settings
# https://odk.gramvaani.org/#/projects/9/forms/Add_Settlements_form%20_V1.0.1
OD_DATA_URL_hemlet = "https://odk.gramvaani.org/v1/projects/9/forms/Add_Hamlet_form%20_V1.0.1.svc/Submissions"
OD_DATA_URL_well = (
    "https://odk.gramvaani.org/v1/projects/9/forms/Add_well_form_V1.0.1.svc/Submissions"
)
OD_DATA_URL_wb = "https://odk.gramvaani.org/v1/projects/9/forms/Hamlet_Waterbodies_Form_V1.0.3.svc/Submissions"
OD_DATA_URL_plan = {
    "odk_prop_agri": {
        "odk_url": "https://odk.gramvaani.org/v1/projects/9/forms/NRM_form_Agri_Screen_V1.0.0.svc/Submissions",
        "gps_point": "GPS_point_irrigation_work",
    },
    "odk_prop_wb": {
        "odk_url": "https://odk.gramvaani.org/v1/projects/9/forms/NRM_form_NRM_form_Waterbody_Screen_V1.0.0.svc/Submissions",
        "gps_point": "GPS_point_propose_maintainence",
    },
    "odk_prop_gw": {
        "odk_url": "https://odk.gramvaani.org/v1/projects/9/forms/NRM_form_propose_new_recharge_structure_V1.0.0.svc/Submissions",
        "gps_point": "GPS_point_recharge_structure",
    },
}

# Report requirements
OVERPASS_URL = env("OVERPASS_URL")

# EMAIL Settings
EMAIL_BACKEND = "django.core.mail.backends.smtp.EmailBackend"
EMAIL_HOST = "smtpout.secureserver.net"
EMAIL_PORT = 465
EMAIL_USE_SSL = True
EMAIL_USE_TLS = False
EMAIL_HOST_USER = env("EMAIL_HOST_USER")
EMAIL_HOST_PASSWORD = env("EMAIL_HOST_PASSWORD")
EMAIL_TIMEOUT = 30

GEOSERVER_URL = env("GEOSERVER_URL")
GEOSERVER_USERNAME = env("GEOSERVER_USERNAME")
GEOSERVER_PASSWORD = env("GEOSERVER_PASSWORD")

EARTH_DATA_USER = env("EARTH_DATA_USER")
EARTH_DATA_PASSWORD = env("EARTH_DATA_PASSWORD")

GEE_SERVICE_ACCOUNT_KEY_PATH = env("GEE_SERVICE_ACCOUNT_KEY_PATH")
GEE_HELPER_SERVICE_ACCOUNT_KEY_PATH = env("GEE_HELPER_SERVICE_ACCOUNT_KEY_PATH")
