from __future__ import print_function
import boto3
import base64
import os
import json

REGION = os.environ['REGION']


def handle(event, context):
    print("START----------------------------------------------START")

    print(event)
    for record in event['Records']:
        try:
            payload = base64.b64decode(record['kinesis']['data'])
            data_item = json.loads(payload)
            print('Handling item to sns')
            print(json.dumps(data_item))
            print('Item successfully Handled')
        except Exception as e:
            print(e)
            print("failure")

    print("END----------------------------------------------END")



