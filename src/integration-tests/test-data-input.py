from s3 import store_data, get_from_s3
import time
import json

INPUT_BUCKET = 'sls-data-pipelines-dev-originbucket-1ayfh2rded747'
RESOURCES_PATH = './test-resources'
S3_XML_PREFIX = 'xml/input/'
S3_JSON_PREFIX = 'json/input/'


def test_when_xml_is_put_on_bucket_it_is_correctly_converted_to_json():
    xml = None
    now = str(int(round(time.time() * 1000)))
    object_key = S3_XML_PREFIX + now + '.xml'
    with open(RESOURCES_PATH + '/input-xml.xml', 'r') as f_xml:
        xml = f_xml.read()

    store_data(xml, object_key, INPUT_BUCKET)

    json_from_s3 = get_from_s3(S3_JSON_PREFIX + now + '.json', INPUT_BUCKET)

    assert json_from_s3 is not None

    input_as_json = json.loads(json_from_s3)

    assert type(input_as_json) is dict
    assert len(input_as_json["miv"]["meetpunt"]) == 2





