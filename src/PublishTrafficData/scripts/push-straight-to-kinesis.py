import boto3
import json
from collections.abc import MutableMapping
import time

firehose_client = boto3.client('firehose')

KINESIS_FIREHOSE_NAME = ''


def transform_event(event_as_string):
    event = json.loads(event_as_string)
    meetdata = event["meetdata"]
    meetdata_dict = generate_meetdata_dict(meetdata)
    event.pop("meetdata")
    event.update(meetdata_dict)
    rekendata_flattened = flatten({"rekendata": event["rekendata"]})
    event.pop("rekendata")
    event.update(rekendata_flattened)
    return json.dumps(event)


def generate_meetdata_dict(meetdata):
    meetdata_dict = {}
    for md in meetdata:
        klasse_id = md.get("klasse_id")
        key1 = 'verkeersintensiteit' + '_klasse' + klasse_id
        key2 = 'voertuigsnelheid_rekenkundig' + '_klasse' + klasse_id
        key3 = 'voertuigsnelheid_harmonisch' + '_klasse' + klasse_id
        meetdata_dict[key1] = md.get('verkeersintensiteit')
        meetdata_dict[key2] = md.get('voertuigsnelheid_rekenkundig')
        meetdata_dict[key3] = md.get('voertuigsnelheid_harmonisch')
    return meetdata_dict


def flatten(d, parent_key='', sep='_'):
    items = []
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, MutableMapping):
            items.extend(flatten(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)


def pretty_output(response):
    return json.dumps(response, indent=4)


def put_record(client, delivery_stream_name, meetpunt):
    for i in range(1, 5000):
        print("put %d" % i)
        transformed_event = transform_event(meetpunt)
        print(transformed_event)
        response = client.put_record(
            DeliveryStreamName=delivery_stream_name,
            Record={
                'Data': transformed_event.encode()
            }
        )
        print(pretty_output(response))
        time.sleep(1)


if __name__ == "__main__":
    meetpunt = None
    with open('event.json', 'r') as f:
        meetpunt = f.read()
    put_record(firehose_client, KINESIS_FIREHOSE_NAME, meetpunt)
