from sqlalchemy import create_engine
from sqlalchemy import schema
import pandas as pd
from ..base import BaseConnector
import os

class PostgresConnector(BaseConnector):
    def __init__(self, config: dict = None):
        self.config = config
        # We store the engine as the main entry point
        self.engine = self.connect(config)
        self._ensure_dataset_exists()

    def connect(self, config: dict):
        user = os.getenv("POSTGRES_USER") or config.get('user')
        password = os.getenv("POSTGRES_PASSWORD")
        host = os.getenv("POSTGRES_HOST") or config.get('host')
        port = os.getenv("POSTGRES_PORT") or config.get('port')
        db = os.getenv("POSTGRES_DB") or config.get('database')

        # SQLAlchemy connection string format:
        # postgresql+psycopg2://user:password@host:port/dbname
        connection_string = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
        
        return create_engine(connection_string)

    def _ensure_schema_exists(self):
        target_schema = self.config.get("default_schema", "public")
        if target_schema != "public":
            with self.engine.connect() as conn:
                # Check if schema exists, if not create it
                conn.execute(schema.CreateSchema(target_schema, if_not_exists=True))
                conn.commit()
    
    def execute_query(self, query: str):
        # With SQLAlchemy, you can use the engine to execute or load into Pandas
        return pd.read_sql(query, self.engine)
    
    def get_data(self, query: str):
        # Use a context manager for the connection
        return pd.read_sql(query, self.engine.connect())

    def load_data(self, df, table_name: str, schema: str = None, if_exists: str = "append"):
        """Loads a dataframe into Postgres."""
        
        # Priority: 1. Function argument, 2. YAML config, 3. Hardcoded default
        target_schema = schema or self.config.get("default_schema", "public")
        
        df.to_sql(
            name=table_name,
            con=self.engine,
            schema=target_schema,
            if_exists=if_exists,
            index=False
        )
        print(f"✅ Loaded {len(df)} rows to Postgres table: {table_name}")