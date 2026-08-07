import os
from pathlib import Path
from dagster import Definitions, define_asset_job, ScheduleDefinition, AssetSelection, load_assets_from_modules
from dagster_dbt import DbtCliResource, dbt_assets
import extract_eskom_data
import pipeline_run_log

# 1. Point Dagster to the dbt project directory
dbt_project_dir = Path(__file__).joinpath("..", "dbt_project").resolve()

# 2. Configure the dbt resource
dbt_resource = DbtCliResource(project_dir=os.fspath(dbt_project_dir))

# 3. Load dbt models and link them to the Python extraction asset
@dbt_assets(
    manifest=dbt_project_dir.joinpath("target", "manifest.json"),
)
def eskom_dbt_assets(context, dbt: DbtCliResource):
    # `dbt build` runs models and their associated tests together
    yield from dbt.cli(["build"], context=context).stream()

# 4. Load the Python extraction assets
extraction_assets = load_assets_from_modules([extract_eskom_data, pipeline_run_log])

# 5. Define the end-to-end pipeline job (extraction → transformation → tests)
eskom_update_job = define_asset_job(
    name="eskom_grid_update_job",
    selection=AssetSelection.all()
)

# 6. Schedule configuration
#
#    Free Tier Budget: 50 API requests/day
#    Areas monitored:  2 (Johannesburg, Cape Town)
#    Calls per run:    2 (1 per area — search step eliminated via areas_config.yml)
#    Runs per day:     24 (hourly) → 48 calls/day → 2 buffer calls remaining
#
#    Cron: "0 * * * *" = top of every hour
#
#    To upgrade to 15-minute intervals (Business tier required):
#      Change PIPELINE_INTERVAL_MINUTES to 15 in .env
interval = int(os.getenv("PIPELINE_INTERVAL_MINUTES", "60"))
cron_str = f"*/{interval} * * * *" if interval < 60 else f"0 */{interval//60} * * *"

eskom_hourly_schedule = ScheduleDefinition(
    name="eskom_grid_schedule",
    job=eskom_update_job,
    cron_schedule=cron_str,
)

# 7. Single workspace Definition — all assets, schedules, and resources
defs = Definitions(
    assets=[*extraction_assets, eskom_dbt_assets],
    schedules=[eskom_hourly_schedule],
    resources={
        "dbt": dbt_resource,
    },
)