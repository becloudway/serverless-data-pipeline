from __future__ import print_function
import boto3
import base64
import os
import json


table = boto3.resource('dynamodb').Table(os.environ["TABLE_NAME"])


def handle(event, context):
    print("START----------------------------------------------START")

    print(event)
    with table.batch_writer() as batch:
        for record in event['Records']:
            try:
                payload = base64.b64decode(record['kinesis']['data'])
                data = json.loads(payload)
                print(json.dumps(data))
                print('Handling item')
                item = None
                if data.get('outputType') == 'SPEED_DIFFERENTIAL':
                    print('type is speed')
                    item = create_speed_item(data)
                elif data.get('outputType') == 'TRAFFIC_JAM':
                    print('type is jam')
                    item = create_traffic_jam_item(data)
                sort_key = create_sort_key(item)
                item["outputType_recordTimestamp"] = sort_key
                batch.put_item(Item=item)
            except Exception as e:
                print(e)
                print("failure")

    print("END----------------------------------------------END")


def create_speed_item(data_item):
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


def create_sort_key(item):
    output_type = item.get('outputType')
    record_timestamp = item.get('recordTimestamp')
    sort_key = output_type + '_' + record_timestamp
    return sort_key
