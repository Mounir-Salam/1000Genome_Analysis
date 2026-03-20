import os
import yaml
import pytest
from dotenv import load_dotenv
from src.db_connection.builder import IOBuilder

# 1. Load secrets from .env as soon as the module is imported
load_dotenv()

def get_storage_names():
    """Helper to get keys from YAML for pytest parametrization."""
    try:
        with open("src/config/databases.yaml", "r") as f:
            full_config = yaml.safe_load(f)
            return list(full_config.get('storage', {}).keys())
    except FileNotFoundError:
        return []

@pytest.mark.parametrize("db_name", get_storage_names())
def test_storage_connectivity(db_name):
    """
    This is the function pytest will look for. 
    The @pytest.mark.parametrize decorator feeds the db_names into it.
    """
    # Load config inside the test
    with open("src/config/databases.yaml", "r") as f:
        full_config = yaml.safe_load(f)
    
    db_config = full_config['storage'].get(db_name)

    if not db_config:
        pytest.fail(f"Database config for '{db_name}' not found!")

    print(f"\n--- Testing {db_name} ({db_config['type']}) ---")

    connector = IOBuilder.get_resource(db_config)

    # List buckets
    print("Buckets:")
    for bucket in connector.client.list_buckets():
        print(f"- {bucket}")
    
    # Assertions are how pytest knows if a test passed
    assert connector.client is not None, f"Connection to {db_name} returned None"
    print(f"✅ Successfully connected to {db_name}!")

if __name__ == "__main__":
    print("Running manual connection checks...")
    
    databases_to_test = [
        "gcs_raw_data"
    ]
    
    for storage in databases_to_test:
        try:
            test_storage_connectivity(storage)
        except Exception as e:
            print(f"❌ Manual test failed for {storage}: {e}")