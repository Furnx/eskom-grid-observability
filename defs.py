import os
from pathlib import Path
from dagster import Definitions, load_assets_from_modules
from dagster_dbt import DbtCliResource, dbt_assets
import extract_eskom_data

# 1. Point Dagster to dbt project directory
dbt_project_dir = Path(__file__).joinpath("..", "dbt_project").resolve()

# 2. Configure the dbt resource
dbt_resource = DbtCliResource(project_dir=os.fspath(dbt_project_dir))

# 3. Load the dbt models and link them to the Python extraction
@dbt_assets(
    manifest=dbt_project_dir.joinpath("target", "manifest.json"),
    deps=[extract_eskom_data.raw_eskom_tshwane_schedule]
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