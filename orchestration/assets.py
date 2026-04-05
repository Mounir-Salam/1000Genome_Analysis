import os
import subprocess
from dagster import asset, Output, AssetExecutionContext
from dagster_dbt import DbtCliResource, dbt_assets
from orchestration.constants import ROOT_DIR, DBT_PROJECT_DIR, DBT_EXECUTABLE

@asset(group_name="raw_data", description="Load raw data into the data warehouse")
def sequence_index(context: AssetExecutionContext):
    script_path = ROOT_DIR / "src" / "scripts" / "sequence_index_processing.py"
    
    context.log.info(f"Running sequence index processing script at {script_path}")
    
    # Use subprocess to run 'uv run python'
    result = subprocess.run(
        ["uv", "run", "python", str(script_path)],
        capture_output=True,
        text=True,
        check=True
    )
    
    context.log.info(result.stdout)
    
    # Return metadata to show in the Dagster UI
    return Output(
        value=None, 
        metadata={
            "script_location": str(script_path),
            "status": "success"
        }
    )
    
@asset(group_name="raw_data", description="Load raw data into the data warehouse")
def population_index(context: AssetExecutionContext):
    script_path = ROOT_DIR / "src" / "scripts" / "population_index_processing.py"
    
    context.log.info(f"Running population index processing script at {script_path}")
    
    # Use subprocess to run 'uv run python'
    result = subprocess.run(
        ["uv", "run", "python", str(script_path)],
        capture_output=True,
        text=True,
        check=True
    )
    
    context.log.info(result.stdout)
    
    # Return metadata to show in the Dagster UI
    return Output(
        value=None, 
        metadata={
            "script_location": str(script_path),
            "status": "success"
        }
    )
    
# Define the dbt Resource pointing to your 3.11 venv
dbt_resource = DbtCliResource(
    project_dir=os.fspath(DBT_PROJECT_DIR),
    dbt_executable=os.fspath(DBT_EXECUTABLE)
)

# Get the target from your environment variable
DB_TARGET = "prod_cloud" if os.getenv("DEFAULT_DB_PLATFORM") == "cloud" else "dev_local"

# Load all dbt models as Dagster assets
# This uses the manifest.json we created with 'dbt parse'
@dbt_assets(
    manifest=DBT_PROJECT_DIR / "target" / "manifest.json",
    dagster_dbt_translator=None
)
def dbt_genome_models(context: AssetExecutionContext, dbt: DbtCliResource):
    context.log.info(f"Using dbt executable at: {dbt.dbt_executable}")
    yield from dbt.cli(
        ["build", "--target", DB_TARGET, "--profiles-dir", os.fspath(DBT_PROJECT_DIR)],
        context=context
    ).stream()