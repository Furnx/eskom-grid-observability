import os
import json
import requests
from dotenv import load_dotenv
from dagster import asset, AssetExecutionContext

load_dotenv()
API_KEY = os.getenv("ESKOM_API_KEY")

@asset(
    name="raw_eskom_tshwane_schedule",
    description="Extracts loadshedding schedule from EskomSePush API v3.0, handles missing events, and saves to local storage.",
    group_name="eskom_extraction",
    compute_kind="python",
)
def raw_eskom_tshwane_schedule(context: AssetExecutionContext) -> None:

    """
    Extracts the live loadshedding and load reduction schedule for Tshwane from the EskomSePush API v3.0.

    This function acts as the ingestion data contract. It intercepts the raw JSON payload
    and normalizes the schema to prevent downstream database compilation errors during
    suspended outage states.

    Data Contract Enforcements:
        - Injects an empty list (`[]`) into the `events` key if the API omits it to
        bypass DuckDB schema inference crashes.
        - Appends a `_meta` dictionary containing `area_id` and `area_name` to decouple
        dimensional logic from volatile API payload structures.

    Args:
        context (AssetExecutionContext): The Dagster execution context used for
            asset-level logging and metadata tracking.

    Raises:
        requests.exceptions.HTTPError: If the API returns a 4xx or 5xx status code.
        requests.exceptions.JSONDecodeError: If the API gateway fails and returns raw HTML
            instead of a valid JSON payload.
        KeyError: If the expected hierarchical structure of the API response fundamentally changes.

    """

    if not API_KEY:
        raise ValueError("Error: Please put your real API key in the .env file!")

    headers = {"token": API_KEY}

    # Search for Tshwane to get its specific Area ID
    search_text = "Tshwane"
    search_url = f"https://developer.sepush.co.za/business/3.0/areas_search?text={search_text}"

    context.log.info(f"Searching for {search_text} Area ID using v3.0 API...")
    search_response = requests.get(search_url, headers=headers)

    # DEBUGGING SECTION
    context.log.info("Raw API Response:")
    context.log.info(search_response.text)
    context.log.info(f"Status Code: {search_response.status_code}")
    # print("Raw API Response Text:")
    # print(repr(search_response.text))
    # -----------------------------

    try:
        search_data = search_response.json()
    except requests.exceptions.JSONDecodeError:
        context.log.error("\nSTOPPING: The API returned something that isn't JSON. Look at the raw response text above to see what it is.")
        raise Exception("JSONDecodeError: on Area Search")


    if "error" in search_data:
        raise Exception(f"API Error on Area Search: {search_data['error']}")

    # Grab the first matching area's ID
    try:
        area_id = search_data['areas'][0]['id']
        area_name = search_data['areas'][0]['name']
        context.log.info(f"\nFound Area: {area_name} (ID: {area_id})")
    except (KeyError, IndexError):
        context.log.error("\nSTOPPING: The structure was off. Look at the raw response above to see why!")
        context.log.info(f"Parsed JSON data: {json.dumps(search_data, indent=2)}")
        raise Exception("Structure mismatch")

    schedule_url = f"https://developer.sepush.co.za/business/3.0/area?id={area_id}"
    context.log.info(f"Fetching schedule for {area_name}...")

    schedule_response = requests.get(schedule_url, headers=headers)
    schedule_data = schedule_response.json()

    if "error" in schedule_data:
        context.log.error(f"\nAPI Error on Schedule fetch: {schedule_data['error']}")
        raise Exception(f"API Error on Schedule fetch: {schedule_data['error']}")

    # If loadshedding is suspended, the API drops the 'events' key.
    # Inject an empty array so DuckDB's schema reader doesn't crash.
    if "events" not in schedule_data:
        context.log.info("No events found in the schedule. Injecting an empty 'events' array to maintain schema consistency.")
        schedule_data["events"] = []

    # Force our own known dimensions into the payload so dbt always has them.
    schedule_data["_meta"] = {
        "area_id": area_id,
        "area_name": area_name
    }

    # Save the raw JSON payload to Local Data Lake
    os.makedirs("data", exist_ok=True)
    file_path = "data/raw_tshwane_schedule.json"

    with open(file_path, "w") as f:
        json.dump(schedule_data, f, indent=4)

    context.log.info(f"Success! Raw schedule saved to {file_path}")