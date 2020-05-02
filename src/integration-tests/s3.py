import pytest
import boto3
from botocore.exceptions import ClientError
import logging
import time

s3_client = boto3.client('s3')
s3_resource = boto3.resource('s3')


def store_data(data, object_key, bucket):
    try:
        response = s3_client.put_object(Body=data, Bucket=bucket, Key=object_key, ContentType='application/xml')
    except ClientError as e:
        logging.error(e)
        return False
    return True


def get_from_s3(key, bucket):
    max_tries = 5
    tries = 0
    while tries < max_tries:
        try:
            obj = s3_resource.Object(bucket, key)
            return obj.get()['Body'].read().decode('utf-8')
        except s3_resource.meta.client.exceptions.NoSuchKey:
            tries += 1
            print(f"No such key in bucket, tries: {tries}")
            if tries < max_tries:
                time.sleep(1)
                continue

