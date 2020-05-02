import boto3
import json
import time

firehose_client = boto3.client('firehose')


def put_record(delivery_stream_name, event):
    response = firehose_client.put_record(
        DeliveryStreamName=delivery_stream_name,
        Record={
            'Data': json.dumps(event).encode()
        }
    )


