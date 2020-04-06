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
S3_XML_PREFIX = os.environ["S3_XML_PREFIX"]


def retrieve_data():
    r = requests.get(URL)
    return r.text


def store_data(data, object_key, bucket):
    try:
        response = s3_client.put_object(Body=data, Bucket=bucket, Key=object_key, ContentType='application/xml')
    except ClientError as e:
        logging.error(e)
        return False
    return True


def handle(event, context):
    xml_data = retrieve_data()

    now = str(int(round(time.time() * 1000)))
    key = S3_XML_PREFIX + now + '.xml'
    store_data(xml_data, key, BUCKET)

    response = {
        "statusCode": 200,
        "body": 'hello'
    }

    return response
