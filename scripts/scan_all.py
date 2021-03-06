import boto3
dynamodb = boto3.resource('dynamodb', region_name="eu-west-1")

table = dynamodb.Table('sls-data-pipelines-dev-AnalyticsResultsTable-1C3GH4VT1IX3O')

response = table.scan()
data = response['Items']

while 'LastEvaluatedKey' in response:
    response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
    data.extend(response['Items'])

print(len(data))