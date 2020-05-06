import boto3
import os
import json
from decimal import Decimal

dynamodb_resource = boto3.resource('dynamodb')
table = dynamodb_resource.Table(os.environ["TABLE_NAME"])


def handle(event, context):
    dynamodb_items = scan_table()
    items = [parse_dynamo_item_to_dict(item) for item in dynamodb_items]
    print(items)
    trafficLocations = create_traffic_locations_list(items)

    response_body = {
        "trafficLocations": trafficLocations
    }
    response = {
        "statusCode": 200,
        "body": json.dumps(response_body)
    }
    return response


def create_traffic_locations_list(items):
    print(items)
    unique_ids = list(set([d["uniqueId"] for d in items]))
    trafficLocations = []
    for id in unique_ids:
        data = {
            id: [i for i in items if i["uniqueId"] == id]
        }
        trafficLocations.append(data)
    return trafficLocations


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
        "ExpressionAttributeNames": {"#a97a0": "uniqueId", "#a97a1": "outputType"}
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
