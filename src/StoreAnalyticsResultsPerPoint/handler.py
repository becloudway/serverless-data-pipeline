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
            save_speed_diff_item(data)
        elif data.get('outputType') == 'TRAFFIC_JAM':
            save_traffic_jam_item(data)
        elif data.get('outputType') == 'SPEED_AVG':
            print("type is speed_avg")
            print(data)
            save_speed_avg_item(data)

    print("END----------------------------------------------END")


def save_speed_diff_item(data_item):
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


def save_speed_avg_item(data_item):
    combo_key = create_outputType_recordTimestamp_key(data_item)
    speed_avg_item = {
        'uniqueId': str(data_item.get('uniqueId')),
        'recordTimestamp': str(data_item.get('recordTimestamp')),
        'outputType': data_item.get('outputType'),
        'outputType_recordTimestamp': combo_key,
        'currentSpeed': data_item.get('currentSpeed'),
        'avgSpeed2Minutes': data_item.get('avgSpeed2Minutes'),
        'avgSpeed10Minutes': data_item.get('avgSpeed10Minutes'),
        'loc': data_item.get('location'),
    }
    print(speed_avg_item)
    with table.batch_writer() as batch:
        print(f"Will save item: {speed_avg_item}")
        batch.put_item(Item=speed_avg_item)


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