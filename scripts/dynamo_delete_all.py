import boto3

TABLE_NAME="sls-data-pipelines-dev-AnalyticsResultsTable-1DASMYSD0S9IR"

dynamodb = boto3.resource('dynamodb', region_name='eu-west-1')
table = dynamodb.Table(TABLE_NAME)


def create_dynamodb_client(region="eu-west-1"):
    return boto3.client("dynamodb", region_name=region)


def create_scan_input():
    return {
        "TableName": TABLE_NAME,
        "ProjectionExpression": "#a97a0,#a97a1",
        "ExpressionAttributeNames": {"#a97a0":"uniqueId", "#a97a1":"outputType_recordTimestamp"}
    }


def execute_scan(input):
    try:
        response = table.scan(**input)
        print("Scan successful.")
        return response
        # Handle response
    except BaseException as error:
        print("Unknown error while scanning: " + error.response['Error']['Message'])


def main():
    # Create the dictionary containing arguments for scan call
    scan_input = create_scan_input()

    # Call DynamoDB's scan API
    scan = execute_scan(scan_input)
    print(scan.keys())
    count = 0
    with table.batch_writer() as batch:
        for item in scan['Items']:
            if count % 50 == 0:
                print("Records deleted: {}".format(count))
            key = {
                'uniqueId': item['uniqueId'],
                'outputType_recordTimestamp': item['outputType_recordTimestamp']
            }
            batch.delete_item(Key=key)
            count = count + 1


if __name__ == "__main__":
    main()
