from __future__ import print_function
import boto3
import os

dynamodb_resource = boto3.resource('dynamodb')
table = dynamodb_resource.Table(os.environ["TABLE_NAME"])


def handle(event, context):
    print("START----------------------------------------------START")

    print('Successfully processed %s records.' % str(len(event['Records'])))

    print(event)
    for record in event['Records']:
        print(record)
        print("new image:")
        print(record["dynamodb"]["NewImage"])
        unique_id = record["dynamodb"]["NewImage"]["uniqueId"]['S']
        alert_indicator = record["dynamodb"]["NewImage"]["alertIndicator"]['S']
        if alert_indicator == "1":
            print(f"There is a traffic jam at location: {unique_id}")
        elif alert_indicator == "0":
            print(f"The traffic jam at location: {unique_id} has disappeared")

    print("END----------------------------------------------END")

