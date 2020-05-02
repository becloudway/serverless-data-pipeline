import boto3
import json
from decimal import Decimal

dynamodb = boto3.resource('dynamodb', region_name="eu-west-1")


def get_table(table_name):
    return dynamodb.Table(table_name)


def scan_table(table_name, input=None):
    table = get_table(table_name)
    response = table.scan()
    data = response['Items']
    while 'LastEvaluatedKey' in response:
        response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'], **input)
        data.extend(response['Items'])
    return data


def delete_all_items(table_name):
    table = get_table(table_name)
    scan_input = create_scan_input(table_name)
    items = scan_table(table_name, input=scan_input)
    count = 0
    with table.batch_writer() as batch:
        for item in items:
            if count % 50 == 0:
                print("Records deleted untill now: {}".format(count))
            key = {
                'uniqueId': item['uniqueId'],
                'outputType_recordTimestamp': item['outputType_recordTimestamp']
            }
            batch.delete_item(Key=key)
            count = count + 1
    print(f"Deleted {count} items")


def create_scan_input(table_name):
    return {
        "TableName": table_name,
        "ProjectionExpression": "#a97a0,#a97a1",
        "ExpressionAttributeNames": {"#a97a0":"uniqueId", "#a97a1":"outputType_recordTimestamp"}
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