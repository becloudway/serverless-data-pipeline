import boto3
from botocore.exceptions import ClientError

from boto3.dynamodb.conditions import Key, Attr

dynamodb = boto3.resource('dynamodb', 'eu-west-1')
table = dynamodb.Table('sls-data-pipelines-dev-AnalyticsResultsTable-TC5J0ZRQDWBF')


def create_dynamodb_client(region="us-east-1"):
    return boto3.client("dynamodb", region_name=region)


def create_scan_input():
    return {
        "TableName": "sls-data-pipelines-dev-AnalyticsResultsTable-TC5J0ZRQDWBF",
        "ProjectionExpression": "#a97a0,#a97a1",
        "ExpressionAttributeNames": {"#a97a0":"uniqueId", "#a97a1":"recordTimestamp"}
    }


def execute_scan(dynamodb_client, input):
    try:
        response = dynamodb_client.scan(**input)
        print("Scan successful.")
        return response
        # Handle response
    except BaseException as error:
        print("Unknown error while scanning: " + error.response['Error']['Message'])


def main():
    # Create the DynamoDB Client with the region you want
    dynamodb_client = create_dynamodb_client(region="eu-west-1")

    # Create the dictionary containing arguments for scan call
    scan_input = create_scan_input()

    # Call DynamoDB's scan API
    scan = execute_scan(dynamodb_client, scan_input)
    print(scan.keys())
    count = 0
    with table.batch_writer() as batch:
        for item in scan['Items']:
            if count % 500 == 0:
                print(count)
            batch.delete_item(Key={
                'uniqueId': item['uniqueId']['S'],
                'recordTimestamp': item['recordTimestamp']['S']
            })
            count = count + 1


if __name__ == "__main__":
    main()
