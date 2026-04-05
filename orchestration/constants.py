import os
from pathlib import Path

# This finds the root of your 1000Genome_Analysis project
ROOT_DIR = Path(__file__).parent.parent.absolute()

# Paths to your dbt assets
DBT_PROJECT_DIR = ROOT_DIR / "dbt_project"
DBT_PROFILES_DIR = DBT_PROJECT_DIR

# The specific dbt executable in your 3.11 venv
DBT_EXECUTABLE = DBT_PROJECT_DIR / "dbt_venv" / "bin" / "dbt"