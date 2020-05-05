from aws.s3 import store_data, get_from_s3
from aws.firehose import put_record
from aws.dynamodb import scan_table, parse_dynamo_item_to_dict, delete_all_items_for_key_uniqueId_outputType, \
    delete_all_items_for_key_uniqueId
import time
import json
from pytest import fixture

INPUT_BUCKET = 'sls-data-pipelines-dev-originbucket-5hxa5803ppha'
KINESIS_FIREHOSE_STREAM_NAME = 'sls-data-pipelines-dev-DeliveryStream-Q7X2Y3M0YUA8'
ANALYTICS_PER_POINT_DYNAMODB_TABLE_NAME = 'sls-data-pipelines-dev-RealTimeAnalyticsPerPointTable-167B4GF1RTPIP'
TRAFFIC_JAM_ALERTS_DYNAMODB_TABLE_NAME = 'sls-data-pipelines-dev-TrafficJamAlertsTable-35XW6S30O49K'

RESOURCES_PATH = './test_resources'
S3_XML_PREFIX = 'xml/input/'
S3_JSON_PREFIX = 'json/input/'
FILTERED_UNIQUE_IDS = ["1897", "957", "3159", "3977", "1065", "569"]


@fixture()
def db_setup():
    delete_all_items_for_key_uniqueId_outputType(ANALYTICS_PER_POINT_DYNAMODB_TABLE_NAME)
    delete_all_items_for_key_uniqueId(TRAFFIC_JAM_ALERTS_DYNAMODB_TABLE_NAME)


def test_when_xml_is_put_on_bucket_it_is_correctly_converted_to_json():
    xml = None
    now = str(int(round(time.time() * 1000)))
    object_key = S3_XML_PREFIX + now + '.xml'
    with open(RESOURCES_PATH + '/input.xml', 'r') as f_xml:
        xml = f_xml.read()

    store_data(xml, object_key, INPUT_BUCKET)

    json_from_s3 = get_from_s3(S3_JSON_PREFIX + now + '.json', INPUT_BUCKET)

    assert json_from_s3 is not None

    input_as_json = json.loads(json_from_s3)

    assert type(input_as_json) is dict
    assert len(input_as_json["miv"]["meetpunt"]) == 2


def test_when_events_are_put_on_firehose_that_match_filter_criteria_then_analytics_results_arrive_in_analytics_per_point_table(db_setup):
    push_pre_configured_traffic_events_to_stream()

    time.sleep(90)  # give time for firehose and analytics app to process all events

    analytics_data = scan_table(ANALYTICS_PER_POINT_DYNAMODB_TABLE_NAME)

    print(analytics_data)
    print(json.dumps(parse_dynamo_item_to_dict(analytics_data[0])))

    assert len(analytics_data) == 2


def test_traffic_jam_is_detected(db_setup):
    unique_id = FILTERED_UNIQUE_IDS[0]
    events = create_list_of_events(unique_id, ["120", "60", "30"])
    push_traffic_events_to_stream(events)

    time.sleep(90)  # give time for firehose and analytics app to process all events

    analytics_data = scan_table(ANALYTICS_PER_POINT_DYNAMODB_TABLE_NAME)
    analytics_items_as_dict = [parse_dynamo_item_to_dict(analytics_item) for analytics_item in analytics_data]
    check_traffic_jam_analytics_for_unique_id(unique_id, analytics_items_as_dict, 1)


def test_speed_diff_slowing_down_is_detected(db_setup):
    unique_id = FILTERED_UNIQUE_IDS[1]
    events = create_list_of_events(unique_id, ["120", "60", "30"])
    push_traffic_events_to_stream(events)

    time.sleep(90)  # give time for firehose and analytics app to process all events

    analytics_data = scan_table(ANALYTICS_PER_POINT_DYNAMODB_TABLE_NAME)
    analytics_items_as_dict = [parse_dynamo_item_to_dict(analytics_item) for analytics_item in analytics_data]
    check_speed_analytics_for_unique_id(unique_id, analytics_items_as_dict, -1)


def test_speed_diff_accelerating_is_detected(db_setup):
    unique_id = FILTERED_UNIQUE_IDS[1]
    events = create_list_of_events(unique_id, ["30", "60", "90"])
    push_traffic_events_to_stream(events)

    time.sleep(90)  # give time for firehose and analytics app to process all events

    analytics_data = scan_table(ANALYTICS_PER_POINT_DYNAMODB_TABLE_NAME)
    analytics_items_as_dict = [parse_dynamo_item_to_dict(analytics_item) for analytics_item in analytics_data]
    check_speed_analytics_for_unique_id(unique_id, analytics_items_as_dict, 1)


def test_can_measurements_without_speed_are_ignored(db_setup):
    unique_id = FILTERED_UNIQUE_IDS[1]
    events = create_list_of_events(unique_id, ['120', '120', '252', '252'])
    push_traffic_events_to_stream(events)

    time.sleep(90)  # give time for firehose and analytics app to process all events

    analytics_data = scan_table(ANALYTICS_PER_POINT_DYNAMODB_TABLE_NAME)

    analytics_items_as_dict = [parse_dynamo_item_to_dict(analytics_item) for analytics_item in analytics_data]
    check_speed_analytics_for_unique_id(unique_id, analytics_items_as_dict, 0)


def check_traffic_jam_analytics_for_unique_id(unique_id, events, expected_status):
    print(events)
    traffic_jam_events = [e for e in events if e['outputType'] == 'TRAFFIC_JAM']
    traffic_jam_events = [e for e in traffic_jam_events if e['uniqueId'] == unique_id]
    print(traffic_jam_events)
    events_indicating_traffic_jam = [e for e in traffic_jam_events if e['trafficJamIndicator'] == expected_status]
    print(events_indicating_traffic_jam)
    assert len(events_indicating_traffic_jam) == 1
    assert events_indicating_traffic_jam[0]['trafficJamIndicator'] == expected_status


def check_speed_analytics_for_unique_id(unique_id, events, expected_speed_diff):
    print(events)
    traffic_jam_events = [e for e in events if e['outputType'] == 'SPEED_DIFFERENTIAL']
    traffic_jam_events = [e for e in traffic_jam_events if e['uniqueId'] == unique_id]
    print(traffic_jam_events)
    events_indicating_traffic_jam = [e for e in traffic_jam_events if e['speedDiffIndicator'] == expected_speed_diff]
    print(events_indicating_traffic_jam)
    assert len(events_indicating_traffic_jam) == 1
    assert events_indicating_traffic_jam[0]['speedDiffIndicator'] == expected_speed_diff


def update_event_timestamp(event):
    now = int(time.time())
    updated_meetpunt = event.copy()
    updated_meetpunt["tijd_waarneming"] = now
    return updated_meetpunt


def push_traffic_events_to_stream(events):
    for event in events:
        updated_event = update_event_timestamp(event)
        put_record(KINESIS_FIREHOSE_STREAM_NAME, updated_event)
        time.sleep(1)


def push_pre_configured_traffic_events_to_stream():
    events = None
    with open(RESOURCES_PATH + '/firehose-input-events.json', 'r') as f_events:
        events = f_events.readlines()
    events = [json.loads(x.strip()) for x in events]
    print(events)
    for event in events:
        updated_event = update_event_timestamp(event)
        put_record(KINESIS_FIREHOSE_STREAM_NAME, updated_event)
        time.sleep(1)
    return events


def create_list_of_events(unique_id, list_speeds):
    return [create_measurement(unique_id, speed) for speed in list_speeds]


def create_measurement(unique_id, vehicle_speed_klasse2):
    return {
     "beschrijvende_id": "H101L20", "unieke_id": unique_id,
     "lve_nr": "18", "tijd_waarneming": 1588320667,
     "tijd_laatst_gewijzigd": "2020-04-06T17:52:20+01:00",
     "actueel_publicatie": "1",
     "beschikbaar": "1",
     "defect": "0",
     "geldig": "0",
     "verkeersintensiteit_klasse1": "0",
     "voertuigsnelheid_rekenkundig_klasse1": "0",
     "voertuigsnelheid_harmonisch_klasse1": "252",
     "verkeersintensiteit_klasse2": "1",
     "voertuigsnelheid_rekenkundig_klasse2": vehicle_speed_klasse2,
     "voertuigsnelheid_harmonisch_klasse2": vehicle_speed_klasse2,
     "verkeersintensiteit_klasse3": "0",
     "voertuigsnelheid_rekenkundig_klasse3": "0",
     "voertuigsnelheid_harmonisch_klasse3": "252",
     "verkeersintensiteit_klasse4": "0",
     "voertuigsnelheid_rekenkundig_klasse4": "0",
     "voertuigsnelheid_harmonisch_klasse4": "252",
     "verkeersintensiteit_klasse5": "5",
     "voertuigsnelheid_rekenkundig_klasse5": "86",
     "voertuigsnelheid_harmonisch_klasse5": "86",
     "rekendata_bezettingsgraad": "6",
     "rekendata_beschikbaarheidsgraad": "100",
     "rekendata_onrustigheid": "86"
     }
