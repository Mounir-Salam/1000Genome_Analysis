import os
import yaml
import pytest
from dotenv import load_dotenv
from src.db_connection.builder import IOBuilder

# 1. Load secrets from .env as soon as the module is imported
load_dotenv()

def get_db_names():
    """Helper to get keys from YAML for pytest parametrization."""
    try:
        with open("src/config/databases.yaml", "r") as f:
            full_config = yaml.safe_load(f)
            return list(full_config.get('databases', {}).keys())
    except FileNotFoundError:
        return []

# 2. The Core Test Logic
@pytest.mark.parametrize("db_name", get_db_names())
def test_db_connectivity(db_name):
    """
    This is the function pytest will look for. 
    The @pytest.mark.parametrize decorator feeds the db_names into it.
    """
    # Load config inside the test
    with open("src/config/databases.yaml", "r") as f:
        full_config = yaml.safe_load(f)
    
    db_config = full_config['databases'].get(db_name)
    
    if not db_config:
        pytest.fail(f"Database config for '{db_name}' not found!")

    print(f"\n--- Testing {db_name} ({db_config['type']}) ---")
    
    # Build and connect
    connector = IOBuilder.get_resource(db_config)
    conn_obj = connector.connect(db_config)
    
    # Assertions are how pytest knows if a test passed
    assert conn_obj is not None, f"Connection to {db_name} returned None"
    print(f"✅ Successfully connected to {db_name}!")

# 3. Manual Execution Block
# This allows you to still run: uv run python tests/test_connections.py
if __name__ == "__main__":
    print("Running manual connection checks...")

    databases_to_test = [
        "local_pg",
        "analytics_bq"
    ]
    
    for db in databases_to_test:
        try:
            test_db_connectivity(db)
        except Exception as e:
            print(f"❌ Manual test failed for {db}: {e}")