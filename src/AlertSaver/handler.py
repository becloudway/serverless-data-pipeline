from __future__ import print_function
import boto3
import os

dynamodb_resource = boto3.resource('dynamodb')
table = dynamodb_resource.Table(os.environ["TABLE_NAME"])


def handle(event, context):
    for record in event['Records']:
        if not record["dynamodb"].get("NewImage"):
            print("Continuing cause no NewImage")
            continue
        if 'trafficJamIndicator' in record["dynamodb"]["NewImage"]:
            unique_id = record["dynamodb"]["NewImage"]["uniqueId"]['S']
            alert_indicator = record["dynamodb"]["NewImage"]["trafficJamIndicator"]['N']
            location = record["dynamodb"]["NewImage"]["loc"]['S']
            save_traffic_jam_alert_item(unique_id, alert_indicator, location)


def save_traffic_jam_alert_item(unique_id, alert_indicator, location):
    try:
        table.update_item(
            Key={
                'uniqueId': unique_id
            },
            UpdateExpression="set alertIndicator = :ai, loc=:l",
            ExpressionAttributeValues={
                ':ai':  alert_indicator,
                ':l': location
            },
            ConditionExpression='alertIndicator <> :ai')
    except dynamodb_resource.meta.client.exceptions.ConditionalCheckFailedException as e:
        None
