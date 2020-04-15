from __future__ import print_function
import boto3
import base64
import os
import json


table = boto3.resource('dynamodb').Table(os.environ["TABLE_NAME"])


def handle(event, context):
    print("START----------------------------------------------START")

    print(event)
    for record in event['Records']:
        try:
            print(record)
            payload = base64.b64decode(record['kinesis']['data'])
            data_item = json.loads(payload)
            speed_aggr_item = create_item(data_item)
            print('Saving item')
            table.put_item(Item=speed_aggr_item)
            print('Item successfully saved')
        except Exception as e:
            print(e)
            print("failure")

    print("END----------------------------------------------END")


def create_item(data_item):
    # outputType_timestamp = data_item.get('outputType') + '_' + timestamp
    speed_aggr_item = {
        'uniqueId': str(data_item.get('uniqueId')),
        'recordTimestamp': str(data_item.get('recordTimestamp')),
        'outputType': data_item.get('outputType'),
        'currentSpeed': data_item.get('currentSpeed'),
        'previousSpeed': data_item.get('previousSpeed'),
        'speedDiffIndicator': data_item.get('speedDiffIndicator'),
        'bezettingsgraad': data_item.get('bezettingsgraad'),
    }
    return speed_aggr_item