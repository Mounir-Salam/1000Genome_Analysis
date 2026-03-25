import os
from dotenv import find_dotenv, load_dotenv

load_dotenv(find_dotenv('.env'))

DEFAULT_STORAGE_PLATFORM = os.getenv('DEFAULT_STORAGE_PLATFORM', 'local')
DEFAULT_DB_PLATFORM = os.getenv('DEFAULT_DB_PLATFORM', 'local')

LOG_LEVEL = os.getenv('LOG_LEVEL', 'CRITICAL')
LOG_LEVEL_ROOT = os.getenv('LOG_LEVEL_ROOT', 'CRITICAL')
LOG_LEVEL_STDOUT = os.getenv('LOG_LEVEL_STDOUT', 'CRITICAL')
LOG_LEVEL_FILE = os.getenv('LOG_LEVEL_FILE', 'CRITICAL')

GOOGLE_APPLICATION_CREDENTIALS = os.getenv('GOOGLE_APPLICATION_CREDENTIALS', 'keys/sa_key.json')

POSTGRES_USER = os.getenv('POSTGRES_USER', 'root')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'root')
POSTGRES_DB = os.getenv('POSTGRES_DB', 'default_db')
POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'localhost')
POSTGRES_PORT = os.getenv('POSTGRES_PORT', 5432)

GCS_CONNECTOR_VERSION = os.getenv('GCS_CONNECTOR_VERSION', 'hadoop3-2.2.22')