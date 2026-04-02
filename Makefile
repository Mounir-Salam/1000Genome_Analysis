# Load variables from .env
include .env
export

.PHONY: help install load_sequence_data load_population_data transform_data run_pipeline

help:
	@echo "Usage: make [target]"
	@echo "  load_sequence_data      : Run sequence ingestion (Python 3.12)"
	@echo "  load_population_data    : Run population ingestion (Python 3.12)"
	@echo "  transform_data : Run dbt transformations (Python 3.11 venv)"
	@echo "  run_pipeline   : Run both in order"

# Step 1: Run the Spark Ingestion using your main environment
load_sequence_data:
	uv run python src/scripts/sequence_index_processing.py

load_population_data:
	uv run python src/scripts/population_index_processing.py

# Dynamic target selection
DB_TARGET = $(if $(filter cloud,$(DEFAULT_DB_PLATFORM)),prod_cloud,dev_local)

# Step 2: Run dbt using the specialized venv
transform_data:
	cd dbt_project && ./dbt_venv/bin/dbt build --target $(DB_TARGET)

docs:
	cd dbt_project && ./dbt_venv/bin/dbt docs generate --target $(DB_TARGET) && ./dbt_venv/bin/dbt docs serve --target $(DB_TARGET)

# Step 3: The "Master" command
run_pipeline: load_sequence_data load_population_data transform_data