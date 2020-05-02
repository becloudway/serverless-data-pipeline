from aws.s3 import store_data, get_from_s3
from aws.firehose import put_record
from aws.dynamodb import scan_table, parse_dynamo_item_to_dict, delete_all_items
import time
import json
from pytest import fixture

INPUT_BUCKET = 'sls-data-pipelines-dev-originbucket-1ayfh2rded747'
KINESIS_FIREHOSE_STREAM_NAME = 'sls-data-pipelines-dev-DeliveryStream-16IMVP3IZ44PI'
DYNAMODB_TABLE_NAME = 'sls-data-pipelines-dev-AnalyticsResultsTable-1C3GH4VT1IX3O'

RESOURCES_PATH = './test_resources'
S3_XML_PREFIX = 'xml/input/'
S3_JSON_PREFIX = 'json/input/'


@fixture()
def db_setup():
    delete_all_items(DYNAMODB_TABLE_NAME)


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


def test_when_events_are_put_on_firehose_that_match_filter_criteria_then_analytics_results_arrive_in_dynamodb(db_setup):
    events = push_traffic_events_to_stream()

    time.sleep(90) # give time for firehose and analytics app to process all events

    analytics_data = scan_table(DYNAMODB_TABLE_NAME)

    print(analytics_data)
    print(json.dumps(parse_dynamo_item_to_dict(analytics_data[0])))

    assert len(analytics_data) == 2*len(events) # 2 types of aggregations


def test_when_events_go_through_analytics_pipeline_the_correct_aggregations_are_done(db_setup):
    events = push_traffic_events_to_stream()

    time.sleep(90)  # give time for firehose and analytics app to process all events

    analytics_data = scan_table(DYNAMODB_TABLE_NAME)
    analytics_items_as_dict = [parse_dynamo_item_to_dict(analytics_item) for analytics_item in analytics_data]
    check_traffic_jam_analytics(analytics_items_as_dict, 4)
    check_speed_differential_analytics(analytics_items_as_dict, 3, 3)


def check_traffic_jam_analytics(events, expected_traffic_jam_events):
    print(events)
    traffic_jam_events = [e for e in events if e['outputType'] == 'TRAFFIC_JAM']
    print(traffic_jam_events)
    events_indicating_traffic_jam = [e for e in traffic_jam_events if e['trafficJamIndicator'] == 1]
    print(events_indicating_traffic_jam)
    assert len(events_indicating_traffic_jam) == expected_traffic_jam_events


def check_speed_differential_analytics(events, number_of_times_significant_speed_decrease, number_of_times_significant_speed_increase):
    speed_differential_events = [e for e in events if e['outputType'] == 'SPEED_DIFFERENTIAL']
    print(speed_differential_events)
    events_indicating_speed_decrease = [e for e in speed_differential_events if e['speedDiffIndicator'] == -1]
    print(events_indicating_speed_decrease)
    events_indicating_speed_increase = [e for e in speed_differential_events if e['speedDiffIndicator'] == 1]
    print(events_indicating_speed_increase)
    assert len(events_indicating_speed_decrease) == number_of_times_significant_speed_decrease
    assert len(events_indicating_speed_increase) == number_of_times_significant_speed_increase


def update_event(event):
    now = int(time.time())
    updated_meetpunt = event.copy()
    updated_meetpunt["tijd_waarneming"] = now
    return updated_meetpunt


def push_traffic_events_to_stream():
    events = None
    with open(RESOURCES_PATH + '/firehose-input-events.json', 'r') as f_events:
        events = f_events.readlines()
    events = [json.loads(x.strip()) for x in events]
    for event in events:
        updated_event = update_event(event)
        put_record(KINESIS_FIREHOSE_STREAM_NAME, updated_event)
        time.sleep(1)
    return events






