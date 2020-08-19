import boto3
import json
from decimal import Decimal

def handle_decimal_type(obj):
    if isinstance(obj, Decimal):
        if float(obj).is_integer():
            return int(obj)
        else:
            return float(obj)
    raise TypeError

dynamodb_client = boto3.resource('dynamodb', region_name='eu-west-1')

my_table = dynamodb_client.Table('sls-data-pipelines-dev-RealTimeAnalyticsPerPointTable-167B4GF1RTPIP')

# filter_expression = Key('id').eq(record_id) &
# Key('timestamp').between(long(start_date), long(end_date))

result_item = []

result_data = my_table.scan(
    # FilterExpression=filter_expression
)

result_item.extend(result_data['Items'])

while 'LastEvaluatedKey' in result_data:
    result_data = my_table.scan(
        # FilterExpression=filter_expression,
        ExclusiveStartKey=result_data['LastEvaluatedKey']
    )

    result_item.extend(result_data['Items'])

with open('scan_result.json', 'w+') as r:
    json.dump(result_data, r, default=handle_decimal_type)