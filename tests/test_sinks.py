"""Tests for the raw landing-zone sinks. No S3: a fake client is injected."""

import json

import pytest

from eskom_grid.sinks import LocalRawSink, S3RawSink, sink_from_uri

PAYLOAD = {"events": [], "_meta": {"area_id": "za_x", "area_name": "X"}}


# ── LocalRawSink ──────────────────────────────────────────────────────────────


def test_local_sink_writes_area_subdirectory_and_timestamped_file(tmp_path):
    sink = LocalRawSink(tmp_path / "raw")

    uri = sink.write("za_x", "20260922_100000", PAYLOAD)

    expected = tmp_path / "raw" / "za_x" / "20260922_100000.json"
    assert uri == str(expected)
    assert expected.is_file()
    assert json.loads(expected.read_text(encoding="utf-8")) == PAYLOAD


def test_local_sink_accumulates_rather_than_overwrites(tmp_path):
    sink = LocalRawSink(tmp_path)
    sink.write("za_x", "20260922_100000", PAYLOAD)
    sink.write("za_x", "20260922_110000", PAYLOAD)
    assert sorted(p.name for p in (tmp_path / "za_x").iterdir()) == [
        "20260922_100000.json",
        "20260922_110000.json",
    ]


# ── S3RawSink ─────────────────────────────────────────────────────────────────


class FakeS3Client:
    def __init__(self):
        self.calls = []

    def put_object(self, **kwargs):
        self.calls.append(kwargs)


def test_s3_sink_puts_object_under_prefix_area_and_run_ts():
    client = FakeS3Client()
    sink = S3RawSink("my-bucket", prefix="raw", client=client)

    uri = sink.write("za_x", "20260922_100000", PAYLOAD)

    assert uri == "s3://my-bucket/raw/za_x/20260922_100000.json"
    assert len(client.calls) == 1
    call = client.calls[0]
    assert call["Bucket"] == "my-bucket"
    assert call["Key"] == "raw/za_x/20260922_100000.json"
    assert call["ContentType"] == "application/json"
    assert json.loads(call["Body"].decode("utf-8")) == PAYLOAD


def test_s3_sink_without_prefix_writes_at_bucket_root():
    sink = S3RawSink("b", prefix="", client=FakeS3Client())
    assert sink.key_for("za_x", "t") == "za_x/t.json"


def test_s3_sink_requires_bucket():
    with pytest.raises(ValueError):
        S3RawSink("")


# ── sink_from_uri ─────────────────────────────────────────────────────────────


def test_uri_dispatch_s3():
    sink = sink_from_uri("s3://landing-bucket/raw")
    assert isinstance(sink, S3RawSink)
    assert sink.bucket == "landing-bucket"
    assert sink.prefix == "raw"


def test_uri_dispatch_s3_nested_prefix_and_trailing_slash():
    sink = sink_from_uri("s3://b/a/b/c/")
    assert isinstance(sink, S3RawSink)
    assert sink.prefix == "a/b/c"


def test_uri_dispatch_local_path():
    sink = sink_from_uri("data/raw")
    assert isinstance(sink, LocalRawSink)
    assert sink.root.name == "raw"


def test_uri_dispatch_rejects_empty():
    with pytest.raises(ValueError):
        sink_from_uri("")
