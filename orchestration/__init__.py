from dagster import Definitions, load_assets_from_modules
from orchestration import assets
from orchestration.assets import dbt_resource

# This automatically finds every function with @asset in your assets.py
all_assets = load_assets_from_modules([assets])

defs = Definitions(
    assets=all_assets,
    resources={
        "dbt": dbt_resource
    }
)