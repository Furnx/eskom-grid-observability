import os
from pathlib import Path
from dagster import Definitions, load_assets_from_modules
from dagster_dbt import DbtCliResource, dbt_assets
import extract_eskom_data

from dagster import Definitions, define_asset_job, ScheduleDefinition, AssetSelection
from dagster_dbt import DbtCliResource, build_schedule_from_dbt_selection
from eskom_extraction import raw_eskom_tshwane_schedule # Assuming this is your extraction asset
from pathlib import Path

# 1. Point Dagster to dbt project directory
dbt_project_dir = Path(__file__).joinpath("..", "dbt_project").resolve()

# 2. Configure the dbt resource
dbt_resource = DbtCliResource(project_dir=os.fspath(dbt_project_dir))

# 3. Load the dbt models and link them to the Python extraction
@dbt_assets(
    manifest=dbt_project_dir.joinpath("target", "manifest.json"),
)
def eskom_dbt_assets(context, dbt: DbtCliResource):
    # This executes `dbt build` (running models and tests together)
    yield from dbt.cli(["build"], context=context).stream()

# 4. Load Python asset
extraction_assets = load_assets_from_modules([extract_eskom_data])

# 5. Define workspace
defs = Definitions(
    assets=[*extraction_assets, eskom_dbt_assets],
    resources={
        "dbt": dbt_resource,
    },
)

# Define a job that selects ALL assets (Python extraction + dbt transformations)
eskom_update_job = define_asset_job(
    name="eskom_grid_update_job",
    selection=AssetSelection.all()
)

# Define a cron schedule (Runs at minute 0 past every 4th hour)
eskom_4hr_schedule = ScheduleDefinition(
    job=eskom_update_job,
    cron_schedule="0 */4 * * *",
)

# Add the schedule to Definitions
defs = Definitions(
    assets=[raw_eskom_tshwane_schedule, eskom_dbt_assets],
    schedules=[eskom_4hr_schedule], # <-- Injected native schedule
    resources={
        "dbt": DbtCliResource(project_dir=os.fspath(dbt_project_dir)),
    },
)