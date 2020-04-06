import json
import boto3
import requests
import logging
import os
import time

from botocore.exceptions import ClientError


s3_client = boto3.client('s3')

URL = os.environ["TRAFFIC_DATA_URL"]
BUCKET = os.environ["BUCKET_NAME"]


def retrieve_data():
    r = requests.get(URL)
    return r.text


def upload_data(data, file_name, bucket, object_name=None):
    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = file_name
    try:
        response = s3_client.put_object(Body=data, Bucket=bucket, Key=object_name, ContentType='application/xml')
    except ClientError as e:
        logging.error(e)
        return False
    return True


def handle(event, context):
    xml_data = retrieve_data()

    now = str(int(round(time.time() * 1000)))
    key = 'xml/input/' + now + '.xml'
    upload_data(xml_data, key, BUCKET)

    response = {
        "statusCode": 200,
        "body": 'hello'
    }

    return response
