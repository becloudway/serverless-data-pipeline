import json
import os
import pytest

from src.StreamProcessor import handler


def test_filter_criteria_returns_true_when_id_in_filtered_list():
    os.environ["FILTER_IDS"] = "50, 60"
    payload = None

    with open('./payload.json', 'r') as f:
        payload = json.loads(f.read())
    payload["unieke_id"] = "60"

    assert handler.matches_filter_criteria(payload) is True, "Test filter criteria failed"


def test_filter_criteria_returns_false_when_id_not_in_filtered_list():
    os.environ["FILTER_IDS"] = "50, 60"
    payload = None

    with open('./payload.json', 'r') as f:
        payload = json.loads(f.read())
    payload["unieke_id"] = "70"

    assert handler.matches_filter_criteria(payload) is False, "Test filter criteria failed"


def test_filter_criteria_returns_true_when_no_restrictions():
    os.environ["FILTER_IDS"] = "*"
    payload = None

    with open('./payload.json', 'r') as f:
        payload = json.loads(f.read())
    payload["unieke_id"] = "70"

    assert handler.matches_filter_criteria(payload) is True, "Test filter criteria failed"


@pytest.mark.parametrize(
    "test_input",
    ["252", "254"],
)
def test_filter_criteria_returns_false_when_no_measurement(test_input):
    os.environ["FILTER_IDS"] = "*"
    payload = None

    with open('./payload.json', 'r') as f:
        payload = json.loads(f.read())
    payload["unieke_id"] = "70"
    payload["voertuigsnelheid_harmonisch_klasse2"] = test_input

    assert handler.matches_filter_criteria(payload) is False, "Test filter criteria failed"


def test_remove_non_measurements():
    payload = None

    with open('./payload.json', 'r') as f:
        payload = json.loads(f.read())

    payload["voertuigsnelheid_rekenkundig_klasse2"] = "120"
    payload["voertuigsnelheid_harmonisch_klasse2"] = "120"
    payload["voertuigsnelheid_rekenkundig_klasse3"] = "86"
    payload["voertuigsnelheid_harmonisch_klasse3"] = "86"
    payload["voertuigsnelheid_harmonisch_klasse1"] = "252"
    payload["voertuigsnelheid_harmonisch_klasse4"] = "252"

    updated_payload = handler.remove_non_measurements(payload)

    assert updated_payload.get("voertuigsnelheid_rekenkundig_klasse2") == "120"
    assert updated_payload.get("voertuigsnelheid_harmonisch_klasse3") == "86"
    assert updated_payload.get("voertuigsnelheid_rekenkundig_klasse1") is None
    assert updated_payload.get("verkeersintensiteit_klasse4") is None


