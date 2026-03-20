import yaml
import os
import pathlib
from .connectors.db.postgres import PostgresConnector
from .connectors.db.bigquery import BigQueryConnector
from .connectors.storage.gcs import GCSConnector

class IOBuilder:
    _map = {
        "postgres": PostgresConnector,
        "bigquery": BigQueryConnector,
        "gcs": GCSConnector
    }
    
    _config = None  # We'll store the YAML data here
    
    @classmethod
    def _load_config(cls):
        """Internal helper to load the YAML once."""
        if cls._config is None:
            # 1. Get the directory where builder.py is located
            # 2. Go up until we hit the 'src' folder level
            current_file = pathlib.Path(__file__).resolve()

            # This points to the project root (where src/ is)
            project_root = current_file.parent.parent.parent

            config_path = project_root / "src" / "config" / "databases.yaml"
        
            if not config_path.exists():
                raise FileNotFoundError(f"Could not find config at {config_path}")
            
            with open(config_path, "r") as f:
                cls._config = yaml.safe_load(f)
        return cls._config
    
    @classmethod
    def get_resource(cls, resource_name: str):
        config = cls._load_config()

        for key in config.keys():
            resource_config = config.get(key, {}).get(resource_name)
            if resource_config:
                break
        
        if not resource_config:
            raise ValueError(f"Resource '{resource_name}' not found in config.")
        
        db_type = resource_config.get("type")
        connector_class = cls._map.get(db_type)
        
        if not connector_class:
            raise ValueError(f"Unsupported DB type: {db_type}")
            
        return connector_class(resource_config)