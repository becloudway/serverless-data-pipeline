import requests
import xmltodict
import json

URL = "http://miv.opendata.belfla.be/miv/configuratie/xml"
FILTER_LIST = ['1897', '957', '3159', '3977', '1065', '569']

def retrieve_data():
    r = requests.get(URL)
    return r.text


def convert_xml_to_dict(xml_data):
    my_dict = xmltodict.parse(xml_data)
    return my_dict


def get_all_meetpunten(dict):
    return dict.get("mivconfig").get("meetpunt")


def write_to_dict_to_disk(dict, filename):
    with open(filename, 'w+') as f:
        f.write(json.dumps(dict))


def write_list_line_by_line_to_disk(list, filename):
    with open(filename, 'w+') as f:
        for el in list:
            f.write(el + '\n')


def create_geo_json(meetpunten_list):
    features = []
    for meetpunt in meetpunten_list:
        filter_pass = filter_meetpunt(meetpunt)
        if not filter_pass:
            continue
        feature = create_geojson_feature(meetpunt)
        features.append(feature)
    geojson = {
        "type": "FeatureCollection",
        "features": features
    }
    return geojson


def create_geojson_feature(meetpunt):
    id = meetpunt.get("@unieke_id")
    name = meetpunt.get("volledige_naam")
    longitude = float(meetpunt.get("lengtegraad_EPSG_4326").replace(",", "."))
    latitude = float(meetpunt.get("breedtegraad_EPSG_4326").replace(",", "."))
    feature = {
        "type": "Feature",
        "id": id,
        "geometry": {
            "type": "Point",
            "coordinates": [
                longitude,
                latitude
            ]
        },
        "properties": {
            "name": name + " - " + id
        }
    }
    return feature


def filter_meetpunt(meetpunt):
    if meetpunt.get("@unieke_id") in FILTER_LIST:
        return True
    return False


def create_csv(meetpunten_list):
    csv_lines = ['locatie-id\tlocatie\tid\tlongitude\tlatitude']
    for meetpunt in meetpunten_list:
        id = meetpunt.get("@unieke_id")
        location_name = meetpunt.get("volledige_naam")
        longitude = str(meetpunt.get("lengtegraad_EPSG_4326").replace(",", "."))
        latitude = str(meetpunt.get("breedtegraad_EPSG_4326").replace(",", "."))
        line = location_name + "-" + id + '\t' + location_name + '\t' + id + '\t' + longitude + '\t' + latitude
        csv_lines.append(line)
    return csv_lines


def run():
    config_xml = retrieve_data()
    config_dict = convert_xml_to_dict(config_xml)
    write_to_dict_to_disk(config_dict, 'configuration.json')
    meetpunten_list = get_all_meetpunten(config_dict)
    meetpunten = {
        'meetpunten': meetpunten_list
    }
    write_to_dict_to_disk(meetpunten, 'meetpunten.json')
    csv_lines = create_csv(meetpunten_list)
    write_list_line_by_line_to_disk(csv_lines, 'meetpunten.csv')
    geojson = create_geo_json(meetpunten_list)
    write_to_dict_to_disk(geojson, 'geojson.json')


if __name__ == "__main__":
    run()
