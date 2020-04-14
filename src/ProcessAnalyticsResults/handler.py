from __future__ import print_function
import boto3
import base64
from json import loads

# table = boto3.resource('dynamodb').Table("trj_info")


def handle(event, context):
    payload = event['records']
    output = []
    success = 0
    failure = 0

    print("START----------------------------------------------START")

    for record in payload:
        payload = base64.b64decode(record['data'])
        print(payload)

        item = loads(payload)

        output.append({'recordId': record['recordId'], 'result': 'Ok'})

    print("END----------------------------------------------END")