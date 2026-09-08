import duckdb
import datetime
import json
from pathlib import Path
from dagster import asset, AssetExecutionContext, Output, MetadataValue

@asset(
    name="pipeline_run_log",
    deps=["raw_eskom_grid_schedules"],
    description="Writes an audit record of the extraction run to a DuckDB table to prove grid uptime.",
    group_name="eskom_extraction",
    compute_kind="python",
)
def pipeline_run_log(context: AssetExecutionContext) -> Output[None]:
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
