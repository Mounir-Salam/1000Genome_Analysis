import os
from src.db_connection.builder import IOBuilder
from src.config import settings
class ResourceManager:
    @staticmethod
    def get_main_storage():
        mode = settings.DEFAULT_STORAGE_PLATFORM
        return IOBuilder.get_resource("gcs_raw_data" if mode == "cloud" else "local_data")

    @staticmethod
    def get_main_db():
        mode = settings.DEFAULT_DB_PLATFORM
        return IOBuilder.get_resource("analytics_bq" if mode == "cloud" else "local_pg")