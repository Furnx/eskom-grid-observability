import os
import json
import yaml
import requests
from pathlib import Path
from dotenv import load_dotenv
from dagster import asset, AssetExecutionContext, Output, MetadataValue

load_dotenv()
API_KEY = os.getenv("ESKOM_API_KEY")

# ---------------------------------------------------------------------------
# Load monitored area portfolio from areas_config.yml
# ---------------------------------------------------------------------------
_CONFIG_PATH = Path(__file__).parent / "areas_config.yml"

def _load_area_config() -> list[dict]:
    """Reads the area portfolio from areas_config.yml at the project root."""
    with open(_CONFIG_PATH, "r") as f:
        config = yaml.safe_load(f)
    return config["areas"]


# ---------------------------------------------------------------------------
# Dagster Asset
# ---------------------------------------------------------------------------
@asset(
    name="raw_eskom_grid_schedules",
    description=(
        "Extracts grid event schedules from the EskomSePush API v3.0 for all "
        "areas defined in areas_config.yml. Enforces a strict data contract to "
        "survive API schema drift and writes one JSON file per area to data/raw/."
    ),
    group_name="eskom_extraction",
    compute_kind="python",
)
def raw_eskom_grid_schedules(context: AssetExecutionContext) -> Output[None]:
    """
    Multi-area ELT extraction worker.

    Iterates over the area portfolio defined in areas_config.yml, fetches the
    live grid event schedule for each area from the EskomSePush API v3.0, and
    writes a schema-normalized JSON file to data/raw/{area_id}.json.

    Data Contract Enforcements:
        - Reads pre-resolved area_ids from areas_config.yml, eliminating the
          /areas_search API call and halving per-run API consumption.
        - Injects an empty list ([]) into the `events` key if the API omits it
          (occurs when no events are scheduled), preventing DuckDB schema crashes.
        - Injects a `_meta` block containing area_id, area_name, municipality,
          and province, decoupling dimensional context from volatile API payloads.

    API Budget (Free Tier = 50 req/day):
        1 request per area per run. At hourly cadence (24 runs/day) with 2 areas
        = 48 calls/day.

    Args:
        context (AssetExecutionContext): Dagster execution context for logging
            and metadata emission.

    Returns:
        Output[None]: Emits Dagster metadata summarising the run (areas processed,
            total events found, zero-event areas).

    Raises:
        ValueError: If ESKOM_API_KEY is not set in the environment.
        Exception: On HTTP 429 (rate limit exceeded) or unexpected API errors.
    """

    if not API_KEY:
        raise ValueError(
            "ESKOM_API_KEY is not set. Add it to the .env file at the project root."
        )

    areas = _load_area_config()
    context.log.info(f"Loaded {len(areas)} area(s) from areas_config.yml.")

    # Ensure output directory exists
    output_dir = Path("data/raw")
    output_dir.mkdir(parents=True, exist_ok=True)

    headers = {"token": API_KEY}
    total_events = 0
    zero_event_areas = []

    for area in areas:
        area_id   = area["area_id"]
        area_name = area["area_name"]
        municipality = area["municipality"]
        province     = area["province"]

        context.log.info(f"Fetching schedule for '{area_name}' ({area_id}) ...")

        schedule_url = f"https://developer.sepush.co.za/business/3.0/area?id={area_id}"
        response = requests.get(schedule_url, headers=headers)

        # ── Rate limit guard ────────────────────────────────────────────────
        if response.status_code == 429:
            context.log.error(
                "HTTP 429 — API daily quota exceeded. "
                "The pipeline will halt to avoid wasting future calls. "
                "Remaining areas will not be fetched this run."
            )
            raise Exception("API rate limit exceeded (HTTP 429). Check quota tomorrow.")

        response.raise_for_status()

        # ── JSON decode guard ────────────────────────────────────────────────
        try:
            payload = response.json()
        except requests.exceptions.JSONDecodeError:
            context.log.error(
                f"API returned non-JSON content for '{area_name}'. "
                f"Raw response: {response.text[:200]}"
            )
            raise Exception(f"JSONDecodeError fetching schedule for {area_name}")

        if "error" in payload:
            raise Exception(f"API error for '{area_name}': {payload['error']}")

        # ── Data Contract: inject empty events array if absent ───────────────
        # When no events are scheduled the API omits the key entirely.
        # An empty list allows DuckDB UNNEST to safely yield zero rows.
        if "events" not in payload:
            context.log.info(
                f"No 'events' key in response for '{area_name}'. "
                "Injecting empty array to maintain schema stability."
            )
            payload["events"] = []
            zero_event_areas.append(area_name)

        event_count = len(payload["events"])
        total_events += event_count
        context.log.info(f"  → {event_count} event(s) found for '{area_name}'.")

        # ── Data Contract: inject dimensional metadata ────────────────────────
        # Decouples geography context from the volatile API payload structure.
        payload["_meta"] = {
            "area_id":      area_id,
            "area_name":    area_name,
            "municipality": municipality,
            "province":     province,
        }

        # ── Write to local data lake ─────────────────────────────────────────
        file_path = output_dir / f"{area_id}.json"
        with open(file_path, "w") as f:
            json.dump(payload, f, indent=4)

        context.log.info(f"  → Saved to {file_path}")

    context.log.info(
        f"Extraction complete. {len(areas)} area(s) processed, "
        f"{total_events} total event(s) found."
    )
    if zero_event_areas:
        context.log.info(
            f"Zero-event areas (grid was up): {zero_event_areas}"
        )

    return Output(
        value=None,
        metadata={
            "areas_processed":   MetadataValue.int(len(areas)),
            "total_events_found": MetadataValue.int(total_events),
            "zero_event_areas":  MetadataValue.text(
                ", ".join(zero_event_areas) if zero_event_areas else "none"
            ),
        },
    )