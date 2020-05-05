from __future__ import print_function
import boto3
import botocore
import base64
import os
import json

dynamodb_resource = boto3.resource('dynamodb')
table = dynamodb_resource.Table(os.environ["TABLE_NAME"])


def handle(event, context):
    print("START----------------------------------------------START")

    print(event)
    for record in event['Records']:
        payload = base64.b64decode(record['kinesis']['data'])
        data = json.loads(payload)
        if data.get('outputType') == 'SPEED_DIFFERENTIAL':
            print('Type is speed')
            print(data)
            save_speed_item(data)
        elif data.get('outputType') == 'TRAFFIC_JAM':
            print(data)
            save_traffic_jam_item(data)

    print("END----------------------------------------------END")


def save_speed_item(data_item):
    combo_key = create_outputType_recordTimestamp_key(data_item)
    try:
        table.update_item(
            Key={
                'uniqueId': str(data_item.get('uniqueId')),
                'outputType': str(data_item.get('outputType'))
            },
            UpdateExpression="set recordTimestamp = :r, currentSpeed=:c, previousSpeed=:p, speedDiffIndicator=:sd, bezettingsgraad=:bg, outputType_recordTimestamp=:otrts, loc=:l",
            ExpressionAttributeValues={
                ':r':  str(data_item.get('recordTimestamp')),
                ':c': data_item.get('currentSpeed'),
                ':p': data_item.get('previousSpeed'),
                ':sd': data_item.get('speedDiffIndicator'),
                ':bg': data_item.get('bezettingsgraad'),
                ':l': data_item.get('location'),
                ':otrts': combo_key
            },
            ConditionExpression='speedDiffIndicator <> :sd')
    except dynamodb_resource.meta.client.exceptions.ConditionalCheckFailedException as e:
        None


def save_traffic_jam_item(data_item):
    combo_key = create_outputType_recordTimestamp_key(data_item)
    try:
        table.update_item(
            Key={
                'uniqueId': str(data_item.get('uniqueId')),
                'outputType': str(data_item.get('outputType'))
            },
            UpdateExpression="set recordTimestamp = :r, currentSpeed=:c, trafficJamIndicator=:tji, outputType_recordTimestamp=:otrts, loc=:l",
            ExpressionAttributeValues={
                ':r':  str(data_item.get('recordTimestamp')),
                ':c': data_item.get('currentSpeed'),
                ':tji': data_item.get('trafficJamIndicator'),
                ':l': data_item.get('location'),
                ':otrts': combo_key
            },
            ConditionExpression='trafficJamIndicator <> :tji')
    except dynamodb_resource.meta.client.exceptions.ConditionalCheckFailedException as e:
        None


def create_outputType_recordTimestamp_key(data_item):
    output_type = data_item.get('outputType')
    record_timestamp = data_item.get('recordTimestamp')
    combo_key = output_type + '_' + str(record_timestamp)
    return combo_key