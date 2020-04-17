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
            body_as_string = record.get('body')
            body = json.loads(body_as_string)
            print(json.dumps(body))

            message_as_string = body.get('Message')
            message = json.loads(message_as_string)
            print(json.dumps(message))
            item = None
            if message.get('outputType') == 'SPEED_DIFFERENTIAL':
                print('type is speed')
                item = create_speed_item(message)
            elif message.get('outputType') == 'TRAFFIC_JAM':
                print('type is jam')
                item = create_traffic_jam_item(message)
            print('Saving item {}'.format(json.dumps(item)))
            table.put_item(Item=item)
            print('Item successfully saved')
        except Exception as e:
            print(e)
            print("failure")

    print("END----------------------------------------------END")


def create_speed_item(data_item):
    # outputType_timestamp = data_item.get('outputType') + '_' + timestamp
    speed_item = {
        'uniqueId': str(data_item.get('uniqueId')),
        'recordTimestamp': str(data_item.get('recordTimestamp')),
        'outputType': data_item.get('outputType'),
        'currentSpeed': data_item.get('currentSpeed'),
        'previousSpeed': data_item.get('previousSpeed'),
        'speedDiffIndicator': data_item.get('speedDiffIndicator'),
        'bezettingsgraad': data_item.get('bezettingsgraad'),
    }
    print('Returning speed item {}'.format(json.dumps(speed_item)))
    return speed_item


def create_traffic_jam_item(data_item):
    traffic_jam_item = {
        'uniqueId': str(data_item.get('uniqueId')),
        'recordTimestamp': str(data_item.get('recordTimestamp')),
        'outputType': data_item.get('outputType'),
        'currentSpeed': data_item.get('currentSpeed'),
        'trafficJamIndicator': data_item.get('trafficJamIndicator')
    }
    print('Returning jam item {}'.format(json.dumps(traffic_jam_item)))
    return traffic_jam_item
