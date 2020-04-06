import json
import boto3
import os
import xmltodict
import logging
from botocore.exceptions import ClientError


s3_resource = boto3.resource('s3')
s3_client = boto3.client('s3')

BUCKET = os.environ["BUCKET_NAME"]
S3_XML_PREFIX = os.environ["S3_XML_PREFIX"]
S3_JSON_PREFIX = os.environ["S3_JSON_PREFIX"]


def handle(event, context):
    s3_event = event["Records"][0]
    object_key = s3_event["s3"]["object"]["key"]

    xml_data = get_from_s3(object_key)
    json_data = convert_xml_to_dict(xml_data)
    json_object_key = create_new_key(object_key)
    store_data(json_data, json_object_key, BUCKET)

    response = {
        "statusCode": 200,
        "body": json.dumps('success!')
    }
    return response


def get_from_s3(key):
    obj = s3_resource.Object(BUCKET, key)
    return obj.get()['Body'].read().decode('utf-8')


def convert_xml_to_dict(xml_data):
    my_dict = xmltodict.parse(xml_data)
    json_data = json.dumps(my_dict)
    return json_data


def create_new_key(previous_key):
    file_name = previous_key.replace(S3_XML_PREFIX, '')
    file_name_without_suffix = os.path.splitext(file_name)[0]
    new_json_object_key = S3_JSON_PREFIX + file_name_without_suffix + '.json'
    return new_json_object_key


def store_data(data, object_key, bucket):
    try:
        response = s3_client.put_object(Body=data, Bucket=bucket, Key=object_key, ContentType='application/json')
    except ClientError as e:
        logging.error(e)
        return False
    return True

