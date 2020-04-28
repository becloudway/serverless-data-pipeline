import json
import boto3
import os
import base64


def handle(event, context):
    output = []

    for record in event['records']:
        print(record['recordId'])
        payload = base64.b64decode(record['data'])

        print(payload)

        # Do custom processing on the payload here
        filter_status = determine_record_filter_status(record)

        output_record = {
            'recordId': record['recordId'],
            'result': filter_status,
            'data': base64.b64encode(payload).decode("utf-8")
        }
        output.append(output_record)

    print('Successfully processed {} records.'.format(len(event['records'])))

    return {'records': output}


def determine_record_filter_status(record):
    return 'Ok'
