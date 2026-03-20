from sqlalchemy import create_engine
from ..base import BaseConnector
import os

class PostgresConnector(BaseConnector):
    def __init__(self, config: dict = None):
        self.config = config
        # We store the engine as the main entry point
        self.engine = self.connect(config)

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

    def execute_query(self, query: str):
        # With SQLAlchemy, you can use the engine to execute or load into Pandas
        import pandas as pd
        return pd.read_sql(query, self.engine)