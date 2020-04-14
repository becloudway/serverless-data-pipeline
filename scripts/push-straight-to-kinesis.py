import boto3
import json
import time

firehose_client = boto3.client('firehose')

KINESIS_FIREHOSE_NAME = 'sls-data-pipelines-dev-DeliveryStream-ME98UYAFP4R0'

car_speeds = [120] * 10 \
             + list(reversed(range(100, 120, 10))) \
             + list(reversed(range(40, 100, 20))) \
             + list(reversed(range(10, 40))) \
             + list(range(10, 50, 15)) \
             + list(range(50, 110, 5))


def pretty_output(response):
    return json.dumps(response, indent=4)


def update_meetpunt(meetpunt, car_speed):
    now = int(time.time())
    updated_meetpunt = meetpunt.copy()
    updated_meetpunt["tijd_waarneming"] = now
    updated_meetpunt["voertuigsnelheid_rekenkundig_klasse2"] = car_speed
    updated_meetpunt["voertuigsnelheid_harmonisch_klasse2"] = car_speed
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
    index = 0
    for speed in car_speeds:
        print("put %d" % index)
        updated_meetpunt = update_meetpunt(json.loads(meetpunt), speed)
        put_record(firehose_client, KINESIS_FIREHOSE_NAME, updated_meetpunt)
        time.sleep(1)
        index += 1


if __name__ == "__main__":
    meetpunt = None
    with open('event.json', 'r') as f:
        meetpunt = f.read()
    put_records_on_stream()
