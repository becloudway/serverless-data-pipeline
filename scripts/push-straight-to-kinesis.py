import boto3
import json
import time

firehose_client = boto3.client('firehose')

KINESIS_FIREHOSE_NAME = 'sls-data-pipelines-dev-DeliveryStream-ME98UYAFP4R0'


def pretty_output(response):
    return json.dumps(response, indent=4)


def update_meetpunt(meetpunt):
    now = int(time.time())
    updated_meetpunt = meetpunt.copy()
    updated_meetpunt["tijd_waarneming"] = now
    return updated_meetpunt


def put_record(client, delivery_stream_name, meetpunt):
        response = client.put_record(
            DeliveryStreamName=delivery_stream_name,
            Record={
                'Data': json.dumps(meetpunt).encode()
            }
        )
        print(pretty_output(response))


def put_records_on_stream():
    for i in range(1, 5000):
        print("put %d" % i)
        updated_meetpunt = update_meetpunt(json.loads(meetpunt))
        put_record(firehose_client, KINESIS_FIREHOSE_NAME, updated_meetpunt)
        time.sleep(2)


if __name__ == "__main__":
    meetpunt = None
    with open('event.json', 'r') as f:
        meetpunt = f.read()
    put_records_on_stream()