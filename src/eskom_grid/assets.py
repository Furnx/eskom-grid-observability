"""Dagster assets — thin adapters around the pure logic.

This is the only module in ``eskom_grid`` that imports Dagster, and nothing
else in the package imports it. Each asset does three things and no more:
resolve configuration from the environment, call the pure function, and
translate its result into Dagster metadata.
"""

# NB: no `from __future__ import annotations` here. It would turn the `context`
# annotation into a string, and Dagster validates that annotation as a real
# class at decoration time.

import datetime
import json
import os
from pathlib import Path

import duckdb
from dagster import AssetExecutionContext, MetadataValue, Output, asset
from dotenv import load_dotenv

from .config import load_areas
from .extract import run_extraction
from .sinks import sink_from_uri

# Where raw payloads land. A directory path (local) or s3://bucket/prefix.
RAW_SINK_ENV = "ESKOM_RAW_SINK"
DEFAULT_RAW_SINK = "data/raw"


@asset(
    name="raw_eskom_grid_schedules",
    description=(
        "Extracts grid event schedules from the EskomSePush API v3.0 for all "
        "areas in the packaged portfolio. Enforces a strict data contract to "
        "survive API schema drift and writes one timestamped JSON file per "
        "area to the configured sink ({area_id}/{YYYYMMDD_HHMMSS}.json) for "
        "historical preservation."
    ),
    group_name="eskom_extraction",
    compute_kind="python",
)
def raw_eskom_grid_schedules(context: AssetExecutionContext) -> Output[None]:
    """Multi-area extraction — the Dagster face of :func:`eskom_grid.extract.run_extraction`.

    Configuration (resolved here, never inside the logic):
        ESKOM_API_KEY   required; from ``.env`` at the project root.
        ESKOM_RAW_SINK  optional; ``data/raw`` (default) or ``s3://bucket/prefix``.

    API budget (Free Tier = 50 req/day): one request per area per run.
    """
    load_dotenv()

    api_key = os.getenv("ESKOM_API_KEY")
    if not api_key:
        raise ValueError(
            "ESKOM_API_KEY is not set. Add it to the .env file at the project root."
        )

    sink = sink_from_uri(os.getenv(RAW_SINK_ENV, DEFAULT_RAW_SINK))
    areas = load_areas()
    context.log.info(f"Loaded {len(areas)} area(s) from the packaged portfolio.")

    summary = run_extraction(areas, api_key, sink, log=context.log)

    return Output(
        value=None,
        metadata={
            "run_ts": MetadataValue.text(summary.run_ts),
            "sink": MetadataValue.text(repr(sink)),
            "areas_processed": MetadataValue.int(summary.areas_processed),
            "total_events_found": MetadataValue.int(summary.total_events),
            "zero_event_areas": MetadataValue.text(
                ", ".join(summary.zero_event_areas) if summary.zero_event_areas else "none"
            ),
            "written": MetadataValue.json(summary.written),
        },
    )


@asset(
    name="pipeline_run_log",
    deps=["raw_eskom_grid_schedules"],
    description="Writes an audit record of the extraction run to a DuckDB table to prove grid uptime.",
    group_name="eskom_extraction",
    compute_kind="python",
)
def pipeline_run_log(context: AssetExecutionContext) -> Output[None]:
    # Moved verbatim from the former pipeline_run_log.py. It scans the local
    # landing zone on disk, so it is a local-only asset for now; making it
    # sink-aware is tracked for Phase 2 of the cloud deployment.
    raw_dir = Path("data/raw")
    areas_processed = 0
    total_events = 0
    zero_event_areas = []

    # Scan area subdirectories — read only the latest file per area
    # to avoid overcounting from accumulated historical files.
    for area_dir in raw_dir.iterdir():
        if not area_dir.is_dir():
            continue

        # Find the most recent JSON file in this area's subdirectory
        json_files = sorted(area_dir.glob("*.json"), reverse=True)
        if not json_files:
            continue

        areas_processed += 1
        latest_file = json_files[0]

        with open(latest_file, "r") as f:
            payload = json.load(f)
            events = payload.get("events", [])
            total_events += len(events)
            if len(events) == 0:
                zero_event_areas.append(payload.get("_meta", {}).get("area_name", area_dir.name))

    # Write to DuckDB
    db_path = "data/eskom_data.duckdb"
    run_timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat()
    zero_areas_str = ", ".join(zero_event_areas)

    conn = duckdb.connect(db_path)

    # Ensure table exists
    conn.execute("""
        CREATE TABLE IF NOT EXISTS pipeline_run_log (
            run_timestamp TIMESTAMP,
            areas_processed INTEGER,
            total_events_found INTEGER,
            zero_event_areas VARCHAR
        )
    """)

    # Insert run record
    conn.execute(
        """
        INSERT INTO pipeline_run_log
        VALUES (CAST(? AS TIMESTAMP), ?, ?, ?)
        """,
        [run_timestamp, areas_processed, total_events, zero_areas_str]
    )
    conn.close()

    context.log.info(f"Logged run: {areas_processed} areas, {total_events} events.")

    return Output(
        value=None,
        metadata={
            "run_timestamp": MetadataValue.text(run_timestamp),
            "areas_processed": MetadataValue.int(areas_processed),
            "total_events": MetadataValue.int(total_events),
        }
    )
