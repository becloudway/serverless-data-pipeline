import json
import os

from dataclasses import dataclass

import pytest

from src.StreamProcessor import handler


def test_filter_criteria():
    assert handler.matches_filter_criteria() is True, "Test filter criteria failed"


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


