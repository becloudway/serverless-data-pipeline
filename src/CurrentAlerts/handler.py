import boto3
import botocore
import base64
import os
import json
from decimal import Decimal

dynamodb_resource = boto3.resource('dynamodb')
table = dynamodb_resource.Table(os.environ["TABLE_NAME"])


def handle(event, context):
    dynamodb_items = scan_table()
    data = [parse_dynamo_item_to_dict(item) for item in dynamodb_items]
    print(data)
    response = {
        "statusCode": 200,
        "body": json.dumps(data)
    }
    return response


def scan_table(input={}):
    response = table.scan(**input)
    data = response['Items']
    while 'LastEvaluatedKey' in response:
        response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'], **input)
        data.extend(response['Items'])
    return data


def create_scan_input(table_name):
    return {
        "TableName": table_name,
        "ProjectionExpression": "#a97a0,#a97a1",
        "ExpressionAttributeNames": {"#a97a0":"uniqueId", "#a97a1":"outputType"}
    }


def parse_dynamo_item_to_dict(item):
    return json.loads(json.dumps(item, indent=4, cls=DecimalEncoder))



# Helper class to convert a DynamoDB item to JSON.
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Decimal):
            if o % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)