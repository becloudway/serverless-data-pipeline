import json
import boto3
import os
import logging


s3_resource = boto3.resource('s3')
s3_client = boto3.client('s3')
firehose_client = boto3.client('firehose')

BUCKET = os.environ["BUCKET_NAME"]
S3_JSON_PREFIX = os.environ["S3_JSON_PREFIX"]
DELIVERY_STREAM_NAME = os.environ["DELIVERY_STREAM_NAME"]


def handle(event, context):
    s3_event = event["Records"][0]
    object_key = s3_event["s3"]["object"]["key"]
    json_data_blob = get_from_s3(object_key)
    json_data = json.loads(json_data_blob)
    push_data_to_firehose(json_data)

    response = {
        "statusCode": 200,
        "body": json.dumps('success!')
    }
    return response


def get_from_s3(key):
    obj = s3_resource.Object(BUCKET, key)
    return obj.get()['Body'].read().decode('utf-8')


def push_data_to_firehose(json_data):
    json_record = json_data["miv"]["meetpunt"][0]
    put_events(json.dumps(json_record))


def put_events(data):
    print('Pushing data to firehose: {}'.format(data))
    response = firehose_client.put_record(
        DeliveryStreamName=DELIVERY_STREAM_NAME,
        Record={
            'Data': data
        }
    )