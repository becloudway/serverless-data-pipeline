import json
import logging
import os

from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError


SLACK_CHANNEL = os.environ['SLACK_CHANNEL']
SLACK_WEBHOOK = os.environ['SLACK_WEBHOOK']


logger = logging.getLogger()
logger.setLevel(logging.INFO)


def handle(event, context):
    for record in event['Records']:
        if not record["dynamodb"].get("NewImage"):
            continue;
        unique_id = record["dynamodb"]["NewImage"]["uniqueId"]['S']
        location = record["dynamodb"]["NewImage"]["loc"]['S']
        alert_indicator = record["dynamodb"]["NewImage"]["alertIndicator"]['S']

        if alert_indicator == "1":
            message = f"*NEW* *traffic jam* at location: *{location}* :sob:"
            send_message(message)
        elif alert_indicator == "0":
            if not record["dynamodb"].get("OldImage"):
                continue
            else:
                message = f"The *traffic jam* at location: *{location}* is *GONE*! :smile:"
                send_message(message)


def send_message(message):
    slack_message = {
        'channel': SLACK_CHANNEL,
        'text': message
    }
    req = Request(SLACK_WEBHOOK, json.dumps(slack_message).encode('utf-8'))
    try:
        response = urlopen(req)
        response.read()
        logger.info("Message posted to %s", slack_message['channel'])
    except HTTPError as e:
        logger.error("Request failed: %d %s", e.code, e.reason)
    except URLError as e:
        logger.error("Server connection failed: %s", e.reason)
