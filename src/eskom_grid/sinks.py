"""Raw landing-zone sinks.

A sink is anything with a ``write(area_id, run_ts, payload) -> uri`` method.
The extraction logic is handed a sink and never learns whether it is writing
to a local directory or to S3. Two implementations are provided; the
extraction code, the Dagster asset and the Lambda handler all use the same
``sink_from_uri`` factory to choose one from a single string.

Layout is identical in both cases:  ``<root>/<area_id>/<run_ts>.json``
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Protocol


class RawSink(Protocol):
    """The contract every sink honours."""

    def write(self, area_id: str, run_ts: str, payload: dict) -> str:
        """Persist one area's payload for one run. Returns the written URI."""
        ...


def _serialise(payload: dict) -> str:
    # indent=4 keeps the landing zone human-readable, as the original did.
    return json.dumps(payload, indent=4)


class LocalRawSink:
    """Writes ``<root>/<area_id>/<run_ts>.json`` on the local filesystem."""

    def __init__(self, root: str | Path) -> None:
        self.root = Path(root)

    def write(self, area_id: str, run_ts: str, payload: dict) -> str:
        area_dir = self.root / area_id
        area_dir.mkdir(parents=True, exist_ok=True)
        path = area_dir / f"{run_ts}.json"
        path.write_text(_serialise(payload), encoding="utf-8")
        return str(path)

    def __repr__(self) -> str:
        return f"LocalRawSink(root={str(self.root)!r})"


class S3RawSink:
    """Writes ``s3://<bucket>/<prefix>/<area_id>/<run_ts>.json``.

    boto3 is imported lazily, on first use, so this module can be imported
    without boto3 installed (the local pipeline never needs it). Pass
    ``client`` to inject a pre-built or fake S3 client.
    """

    def __init__(self, bucket: str, prefix: str = "raw", client: Any = None) -> None:
        if not bucket:
            raise ValueError("S3RawSink requires a bucket name.")
        self.bucket = bucket
        self.prefix = prefix.strip("/")
        self._client = client

    @property
    def client(self) -> Any:
        if self._client is None:
            import boto3  # local import: only needed when this sink is used

            self._client = boto3.client("s3")
        return self._client

    def key_for(self, area_id: str, run_ts: str) -> str:
        parts = [self.prefix, area_id, f"{run_ts}.json"] if self.prefix else [area_id, f"{run_ts}.json"]
        return "/".join(parts)

    def write(self, area_id: str, run_ts: str, payload: dict) -> str:
        key = self.key_for(area_id, run_ts)
        self.client.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=_serialise(payload).encode("utf-8"),
            ContentType="application/json",
        )
        return f"s3://{self.bucket}/{key}"

    def __repr__(self) -> str:
        return f"S3RawSink(bucket={self.bucket!r}, prefix={self.prefix!r})"


def sink_from_uri(uri: str) -> RawSink:
    """Build a sink from a location string.

    ``s3://bucket/prefix``  → :class:`S3RawSink`
    anything else          → :class:`LocalRawSink` (treated as a directory path)
    """
    if not uri:
        raise ValueError("sink URI is empty.")
    if uri.startswith("s3://"):
        bucket, _, prefix = uri[len("s3://"):].partition("/")
        return S3RawSink(bucket=bucket, prefix=prefix)
    return LocalRawSink(uri)
