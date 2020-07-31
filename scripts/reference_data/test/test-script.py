from reference_data.generate_reference_and_geojson import create_geojson_feature


def test_create_geojson_feature_creates_correct_feature_for_meetpunt():
    meetpunt = {
        "@unieke_id": "3640",
        "beschrijvende_id": "H291L10",
        "volledige_naam": "Parking Kruibeke",
        "Ident_8": "A0140002",
        "lve_nr": "437",
        "Kmp_Rsys": "94,695",
        "Rijstrook": "R10",
        "X_coord_EPSG_31370": "144474,5297",
        "Y_coord_EPSG_31370": "208293,5324",
        "lengtegraad_EPSG_4326": "4,289731136",
        "breedtegraad_EPSG_4326": "51,18460764"
    }

    feature = create_geojson_feature(meetpunt)

    assert feature.get('id') == "3640"
    assert feature.get('properties').get('name') == "Parking Kruibeke - 3640"
    assert feature.get('geometry').get('coordinates')[0] == 4.289731136
    assert feature.get('geometry').get('coordinates')[1] == 51.18460764
