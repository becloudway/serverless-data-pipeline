import json
import boto3
import os
import logging
from collections.abc import MutableMapping


s3_resource = boto3.resource('s3')
s3_client = boto3.client('s3')
firehose_client = boto3.client('firehose')


BUCKET = os.environ["BUCKET_NAME"]
S3_JSON_PREFIX = os.environ["S3_JSON_PREFIX"]
DELIVERY_STREAM_NAME = os.environ["DELIVERY_STREAM_NAME"]


def handle(event, context):
    s3_event = event["Records"][0]
    object_key = s3_event["s3"]["object"]["key"]
    json_data_blob = get_from_s3(object_key)
    json_data = json.loads(json_data_blob)
    meetpunten = json_data["miv"]["meetpunt"]
    events = transform_meetpunten_to_events(meetpunten)
    print('Sending {} events to firehose'.format(len(events)))
    push_data_to_firehose(events)

    response = {
        "statusCode": 200,
        "body": json.dumps('success!')
    }
    return response


def get_from_s3(key):
    obj = s3_resource.Object(BUCKET, key)
    return obj.get()['Body'].read().decode('utf-8')


def transform_meetpunten_to_events(meetpunten):
    events = []
    for mp in meetpunten:
        event = transform_meetpunt_to_event(mp)
        events.append(event)
    return events


def transform_meetpunt_to_event(meetpunt):
    event = meetpunt
    event = clean_keys(event)
    meetdata = event["meetdata"]
    meetdata_dict = generate_meetdata_dict(meetdata)
    event.pop("meetdata")
    event.update(meetdata_dict)
    rekendata_flattened = flatten({"rekendata": event["rekendata"]})
    event.pop("rekendata")
    event.update(rekendata_flattened)
    return event


def generate_meetdata_dict(meetdata):
    meetdata_dict = {}
    for md in meetdata:
        klasse_id = md.get("klasse_id")
        verkeersintensiteit_key, voertuigsnelheid_rekenkundig_key, voertuigsnelheid_harmonisch_key = create_unique_klasse_keys(klasse_id)
        meetdata_dict[verkeersintensiteit_key] = md.get('verkeersintensiteit')
        meetdata_dict[voertuigsnelheid_rekenkundig_key] = md.get('voertuigsnelheid_rekenkundig')
        meetdata_dict[voertuigsnelheid_harmonisch_key] = md.get('voertuigsnelheid_harmonisch')
    return meetdata_dict


def create_unique_klasse_keys(klasse_id):
    key1 = 'verkeersintensiteit' + '_klasse' + klasse_id
    key2 = 'voertuigsnelheid_rekenkundig' + '_klasse' + klasse_id
    key3 = 'voertuigsnelheid_harmonisch' + '_klasse' + klasse_id
    return key1, key2, key3


def clean_keys(md):
    md_as_string = json.dumps(md)
    md_as_string_updated = md_as_string.replace('@', '')
    md = json.loads(md_as_string_updated)
    return md


def flatten(d, parent_key='', sep='_'):
    items = []
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, MutableMapping):
            items.extend(flatten(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)


def push_data_to_firehose(events):
    number_of_events_sent = 0
    number_of_events_to_send = len(events)
    batch = []
    for i in range(number_of_events_to_send):
        record = {'Data': json.dumps(events[i])}
        batch.append(record)
        if len(batch) == 100 or i == number_of_events_to_send - 1:
            put_batch(batch)
            number_of_events_sent += len(batch)
            batch = []

        if (i+1) % 100 == 0 or i == number_of_events_to_send - 1:
            print('Number of events sent: {}'.format(number_of_events_sent))


def put_batch(batch):
    response = firehose_client.put_record_batch(
        DeliveryStreamName=DELIVERY_STREAM_NAME,
        Records=batch
    )

