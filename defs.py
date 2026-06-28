from dagster import Definitions, load_assets_from_modules
import extract_eskom_data

# Load all functions decorated with @asset from my script
extraction_assets = load_assets_from_modules([extract_eskom_data])

defs = Definitions(
    assets=extraction_assets,
)