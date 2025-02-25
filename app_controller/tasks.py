import copy
import json
import logging
from asyncio.log import logger
from datetime import datetime

import celery
from celery import shared_task


@shared_task(name="tasks.TestSumTask")
def Reading(a, b):
    print("sum: " + str(a + b))
