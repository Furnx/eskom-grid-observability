import os
from pathlib import Path
from dagster import Definitions, define_asset_job, ScheduleDefinition, AssetSelection, load_assets_from_modules
from dagster_dbt import DbtCliResource, dbt_assets
import extract_eskom_data

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

# 4. Load Python asset (Correctly pulling from your actual file)
extraction_assets = load_assets_from_modules([extract_eskom_data])

# 5. Define a job that selects ALL assets (Python extraction + dbt transformations)
eskom_update_job = define_asset_job(
    name="eskom_grid_update_job",
    selection=AssetSelection.all()
)

# 6. Define a cron schedule (Runs at minute 0 past every 4th hour)
eskom_4hr_schedule = ScheduleDefinition(
    job=eskom_update_job,
    cron_schedule="0 */4 * * *",
)

# 7. Define ONE single workspace Definition
defs = Definitions(
    assets=[*extraction_assets, eskom_dbt_assets],
    schedules=[eskom_4hr_schedule], 
    resources={
        "dbt": dbt_resource,
    },
)