from src.integration_tests.s3 import store_data, get_from_s3
from src.integration_tests.firehose import put_record
from src.integration_tests.dynamodb import scan_table, parse_dynamo_item_to_dict, delete_all_items
import time
import json

INPUT_BUCKET = 'sls-data-pipelines-dev-originbucket-1ayfh2rded747'
KINESIS_FIREHOSE_STREAM_NAME = 'sls-data-pipelines-dev-DeliveryStream-16IMVP3IZ44PI'
DYNAMODB_TABLE_NAME = 'sls-data-pipelines-dev-AnalyticsResultsTable-1C3GH4VT1IX3O'

RESOURCES_PATH = './test_resources'
S3_XML_PREFIX = 'xml/input/'
S3_JSON_PREFIX = 'json/input/'


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


def test_when_events_are_put_on_firehose_that_match_filter_criteria_then_analytics_results_arrive_in_dynamodb():
    #Clean dynamo
    delete_all_items(DYNAMODB_TABLE_NAME)
    events = None
    with open(RESOURCES_PATH + '/firehose-input-events.json', 'r') as f_events:
        events = f_events.readlines()
    events = [json.loads(x.strip()) for x in events]
    for event in events:
        updated_event = update_event(event)
        put_record(KINESIS_FIREHOSE_STREAM_NAME, updated_event)
        time.sleep(1)

    time.sleep(90) # give time for firehose and analytics app to process all events

    data = scan_table(DYNAMODB_TABLE_NAME)

    print(type(data))
    print(data)
    print(json.dumps(parse_dynamo_item_to_dict(data[0])))

    assert len(data) == 2*len(events) # 2 types of aggregations


def update_event(event):
    now = int(time.time())
    updated_meetpunt = event.copy()
    updated_meetpunt["tijd_waarneming"] = now
    return updated_meetpunt








