import boto3
import json
import time
import os

firehose_client = boto3.client('firehose')

KINESIS_FIREHOSE_NAME = 'sls-data-pipelines-dev-DeliveryStream-ME98UYAFP4R0'

car_speeds = [120] * 10 \
             + list(range(120, 100, -5)) \
             + list(range(100, 40, -10)) \
             + list(range(40, 10, -2))
# + list(reversed(range(10, 40))) \
# + list(range(10, 50, 10)) \
# + list(range(50, 110, 5))


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
    print(json.dumps(meetpunt))
    response = client.put_record(
        DeliveryStreamName=delivery_stream_name,
        Record={
            'Data': json.dumps(meetpunt).encode()
        }
    )
    print(pretty_output(response))


def put_records_on_stream(list_of_meetpunten):
    index = 0
    for speed in car_speeds:
        print("put %d" % index)
        for meetpunt in list_of_meetpunten:
            updated_meetpunt = update_meetpunt(meetpunt, speed)
            put_record(firehose_client, KINESIS_FIREHOSE_NAME, updated_meetpunt)
            save_event_that_was_sent(updated_meetpunt)
        time.sleep(1)
        index += 1


def create_diff_meetpunten(meetpunt):
    meetpunten = []
    meetpunt1 = meetpunt
    meetpunten.append(meetpunt1)
    # meetpunt2 = meetpunt1.copy()
    # meetpunt2["unieke_id"] = 100
    # meetpunten.append(meetpunt2)
    return meetpunten


def save_event_that_was_sent(event):
    with open('events-sent.json', 'a+') as f:
        f.write(json.dumps(event))
        f.write('\n')


if __name__ == "__main__":
    os.remove('events-sent.json')
    meetpunt = None
    with open('event.json', 'r') as f:
        meetpunt = f.read()
    list_of_meetpunten = create_diff_meetpunten(json.loads(meetpunt))
    put_records_on_stream(list_of_meetpunten)
