from __future__ import print_function
import boto3
import base64
import os
import json

REGION = os.environ['REGION']

sns_client = boto3.client('sns', region_name=REGION)
sns_topic_arn = os.environ["TOPIC_ARN_FAN_OUT"]


def handle(event, context):
    print("START----------------------------------------------START")

    print(event)
    for record in event['Records']:
        try:
            payload = base64.b64decode(record['kinesis']['data'])
            data_item = json.loads(payload)
            print('Publishing item to sns')
            publish_to_sns(data_item)
            print('Item successfully saved')
        except Exception as e:
            print(e)
            print("failure")

    print("END----------------------------------------------END")


def publish_to_sns(message):
    response = sns_client.publish(
        TargetArn=sns_topic_arn,
        Message=json.dumps(message)
    )
    # response = sns_client.publish(
    #     TargetArn=sns_topic_arn,
    #     Message=json.dumps({'default': json.dumps(message)}),
    #     MessageStructure='json'
    # )