import boto3
import requests
import base64
import os
import json

STEAMING_DATASET_URL = os.environ.get('STEAMING_DATASET_URL')

def handle(event, context):
    for record in event['Records']:
        payload = base64.b64decode(record['kinesis']['data'])
        data = json.loads(payload)
        composed_key = create_outputType_recordTimestamp_composed_key(data)
        data["outputType_recordTimestamp"] = composed_key
        print(f"Data that will be forwarded {json.dumps(data)}")
        forward_to_pbi(data)


def create_outputType_recordTimestamp_composed_key(data_item):
    output_type = data_item.get('outputType')
    record_timestamp = data_item.get('recordTimestamp')
    combo_key = output_type + '_' + str(record_timestamp)
    return combo_key


def forward_to_pbi(data):
    # data dict must be contained in a list
    payload = [data]

    # post/push data to the streaming API
    headers = {
        "Content-Type": "application/json"
    }
    response = requests.request(
        method="POST",
        url=STEAMING_DATASET_URL,
        headers=headers,
        data=json.dumps(payload)
    )

    print(response)
    print(response.content)