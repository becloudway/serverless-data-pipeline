from __future__ import print_function
import boto3
import os

dynamodb_resource = boto3.resource('dynamodb')
table = dynamodb_resource.Table(os.environ["TABLE_NAME"])


def handle(event, context):
    print("START----------------------------------------------START")

    print('Successfully processed %s records.' % str(len(event['Records'])))

    for record in event['Records']:
        print(record["dynamodb"]["NewImage"])
        if 'trafficJamIndicator' in record["dynamodb"]["NewImage"]:
            unique_id = record["dynamodb"]["NewImage"]["uniqueId"]['S']
            alert_indicator = record["dynamodb"]["NewImage"]["trafficJamIndicator"]['N']
            save_traffic_jam_alert_item(unique_id, alert_indicator)
    print("END----------------------------------------------END")


def save_traffic_jam_alert_item(unique_id, alert_indicator):
    try:
        table.update_item(
            Key={
                'uniqueId': unique_id
            },
            UpdateExpression="set alertIndicator = :ai",
            ExpressionAttributeValues={
                ':ai':  alert_indicator,
            },
            ConditionExpression='alertIndicator <> :ai')
    except dynamodb_resource.meta.client.exceptions.ConditionalCheckFailedException as e:
        None
