"""Tests for the pure extraction logic. No network: ``requests.get`` is mocked."""

from unittest.mock import Mock, patch

import pytest
import requests

from eskom_grid.extract import (
    ApiError,
    RateLimitError,
    fetch_area_schedule,
    normalize_payload,
    run_extraction,
)

AREA = {
    "area_id": "za_gt_jhb_johannesburg_9hfs",
    "area_name": "Johannesburg",
    "municipality": "City of Johannesburg",
    "province": "Gauteng",
}
AREA_2 = {
    "area_id": "za_wc_cpt_capetowncbd_utix",
    "area_name": "Cape Town CBD",
    "municipality": "City of Cape Town",
    "province": "Western Cape",
}
EVENT = {"start": "2026-09-22T10:00:00+02:00", "end": "2026-09-22T12:30:00+02:00", "note": "Stage 4"}


class RecordingSink:
    """A sink that remembers what it was asked to write."""

    def __init__(self):
        self.writes = []

    def write(self, area_id, run_ts, payload):
        self.writes.append((area_id, run_ts, payload))
        return f"memory://{area_id}/{run_ts}.json"


def fake_response(status=200, json_body=None, text=""):
    response = Mock()
    response.status_code = status
    response.text = text
    if json_body is None:
        response.json.side_effect = requests.exceptions.JSONDecodeError("no json", text, 0)
    else:
        response.json.return_value = json_body
    if status >= 400:
        response.raise_for_status.side_effect = requests.HTTPError(f"HTTP {status}")
    else:
        response.raise_for_status.return_value = None
    return response


# ── normalize_payload ─────────────────────────────────────────────────────────


def test_missing_events_key_is_injected_as_empty_list():
    payload, injected = normalize_payload({"info": {"name": "Johannesburg"}}, AREA)
    assert injected is True
    assert payload["events"] == []


def test_present_events_are_left_alone():
    payload, injected = normalize_payload({"events": [EVENT]}, AREA)
    assert injected is False
    assert payload["events"] == [EVENT]


def test_meta_block_carries_all_four_fields():
    payload, _ = normalize_payload({"events": []}, AREA)
    assert payload["_meta"] == {
        "area_id": AREA["area_id"],
        "area_name": "Johannesburg",
        "municipality": "City of Johannesburg",
        "province": "Gauteng",
    }


def test_normalize_does_not_mutate_input():
    original = {"info": {}}
    normalize_payload(original, AREA)
    assert "events" not in original
    assert "_meta" not in original


# ── fetch_area_schedule ───────────────────────────────────────────────────────


@patch("eskom_grid.extract.requests.get")
def test_fetch_sends_token_header_and_timeout(mock_get):
    mock_get.return_value = fake_response(json_body={"events": []})
    fetch_area_schedule(AREA["area_id"], "secret-token", timeout=7)
    _, kwargs = mock_get.call_args
    assert kwargs["headers"] == {"token": "secret-token"}
    assert kwargs["params"] == {"id": AREA["area_id"]}
    assert kwargs["timeout"] == 7


@patch("eskom_grid.extract.requests.get")
def test_http_429_raises_rate_limit_error(mock_get):
    mock_get.return_value = fake_response(status=429, text="quota")
    with pytest.raises(RateLimitError):
        fetch_area_schedule(AREA["area_id"], "token")


@patch("eskom_grid.extract.requests.get")
def test_non_json_body_raises_api_error(mock_get):
    mock_get.return_value = fake_response(json_body=None, text="<html>down</html>")
    with pytest.raises(ApiError):
        fetch_area_schedule(AREA["area_id"], "token")


@patch("eskom_grid.extract.requests.get")
def test_error_payload_raises_api_error(mock_get):
    mock_get.return_value = fake_response(json_body={"error": "Invalid area"})
    with pytest.raises(ApiError, match="Invalid area"):
        fetch_area_schedule(AREA["area_id"], "token")


@patch("eskom_grid.extract.requests.get")
def test_other_http_errors_propagate(mock_get):
    mock_get.return_value = fake_response(status=500, json_body={})
    with pytest.raises(requests.HTTPError):
        fetch_area_schedule(AREA["area_id"], "token")


# ── run_extraction ────────────────────────────────────────────────────────────


@patch("eskom_grid.extract.requests.get")
def test_run_writes_every_area_with_one_shared_run_ts(mock_get):
    mock_get.side_effect = [
        fake_response(json_body={"events": [EVENT, EVENT]}),
        fake_response(json_body={"info": {}}),  # no events key → zero-event area
    ]
    sink = RecordingSink()

    summary = run_extraction([AREA, AREA_2], "token", sink, run_ts="20260922_100000")

    assert summary.areas_processed == 2
    assert summary.total_events == 2
    assert summary.zero_event_areas == ["Cape Town CBD"]
    assert [w[0] for w in sink.writes] == [AREA["area_id"], AREA_2["area_id"]]
    assert {w[1] for w in sink.writes} == {"20260922_100000"}
    assert summary.written == [
        f"memory://{AREA['area_id']}/20260922_100000.json",
        f"memory://{AREA_2['area_id']}/20260922_100000.json",
    ]
    assert sink.writes[1][2]["events"] == []
    assert sink.writes[1][2]["_meta"]["province"] == "Western Cape"


@patch("eskom_grid.extract.requests.get")
def test_explicit_empty_events_counts_as_zero_event_area(mock_get):
    mock_get.return_value = fake_response(json_body={"events": []})
    summary = run_extraction([AREA], "token", RecordingSink())
    assert summary.zero_event_areas == ["Johannesburg"]


@patch("eskom_grid.extract.requests.get")
def test_rate_limit_stops_the_run_and_writes_nothing_further(mock_get):
    mock_get.side_effect = [
        fake_response(json_body={"events": [EVENT]}),
        fake_response(status=429),
    ]
    sink = RecordingSink()
    with pytest.raises(RateLimitError):
        run_extraction([AREA, AREA_2], "token", sink)
    assert len(sink.writes) == 1  # first area landed; second never fetched


def test_empty_api_key_is_rejected_before_any_request():
    with pytest.raises(ValueError, match="api_key"):
        run_extraction([AREA], "", RecordingSink())


def test_empty_portfolio_is_rejected():
    with pytest.raises(ValueError, match="areas"):
        run_extraction([], "token", RecordingSink())
