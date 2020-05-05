import json
import boto3
import os
import base64

klasses = [
    "klasse1",
    "klasse2",
    "klasse3",
    "klasse4",
    "klasse5",
]

traffic_prefixes = {
    "traffic_intensity_prefix": "verkeersintensiteit",
    "speed_math_prefix": "voertuigsnelheid_rekenkundig",
    "speed_harmonic_prefix": "voertuigsnelheid_harmonisch"
}


def handle(event, context):
    output = []

    for record in event['records']:
        payload = base64.b64decode(record['data'])

        # print(payload.decode("utf-8"))
        result = 'Dropped'

        # Do custom processing on the payload here
        result, updated_payload = check_and_update_payload(json.loads(payload.decode("utf-8")), result)

        output_record = {
            'recordId': record['recordId'],
            'result': result,
            'data': base64.b64encode(json.dumps(updated_payload).encode("utf-8")).decode("utf-8")
        }
        output.append(output_record)

    print('Successfully processed {} records.'.format(len(event['records'])))

    return {'records': output}


def check_and_update_payload(payload, result):
    should_forward = matches_filter_criteria(payload)
    updated_payload = None
    if should_forward:
        result = 'Ok'
        updated_payload = remove_non_measurements(payload)
        print(f"Updated payload is {updated_payload}")
    return result, updated_payload


def matches_filter_criteria(payload=None):
    if payload.get(traffic_prefixes.get("speed_harmonic_prefix") + "_" + klasses[1]) in ['252', '254']:
        return False
    filter_ids = os.environ["FILTER_IDS"]
    if filter_ids == '*':
        return True
    else:
        filter_ids = filter_ids.replace(" ", "")
        permitted_ids = filter_ids.split(",")
        if payload.get("unieke_id") in permitted_ids:
            payload_id = payload.get("unieke_id")
            print(f"Payload matched cause it has id: {payload_id}")
            print("Permitted ids are {}".format(permitted_ids))
            print("Forwarding payload")
            print(json.dumps(payload))
            return True
    return False


def remove_non_measurements(payload):
    for k in klasses:
        harmonic_key = traffic_prefixes.get("speed_harmonic_prefix") + "_" + k
        if payload.get(harmonic_key) == '252':
            remove_elements_for_klasse(payload, k)
    return payload


def remove_elements_for_klasse(payload, k):
    for v in traffic_prefixes.values():
        payload.pop(v + '_' + k)

