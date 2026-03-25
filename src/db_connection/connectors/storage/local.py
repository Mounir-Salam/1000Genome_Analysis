import os
import pathlib
import shutil
from contextlib import contextmanager

class LocalFileSystemConnector:
    def __init__(self, config: dict):
        self.config = config
        # 'bucket_name' in GCS becomes a 'base_directory' in local
        self.base_path = pathlib.Path(config.get("base_path", "data"))
        self.connect()

    def connect(self, config=None):
        # Ensure the local "bucket" (directory) exists
        self.base_path.mkdir(parents=True, exist_ok=True)
        return self.base_path

    def list_blobs(self, prefix=""):
        """Lists files in the local directory, similar to GCS list_blobs."""
        search_path = self.base_path / prefix
        if not search_path.exists():
            return []
        
        # Return relative paths to mimic GCS blob names
        return [str(p.relative_to(self.base_path)) for p in search_path.rglob("*") if p.is_file()]

    @contextmanager
    def open_file(self, target_name, mode='rb'):
        full_path = self.base_path / target_name
        if not full_path.exists():
            raise FileNotFoundError(f"Local file not found: {full_path}")

        f = open(full_path, mode)
        try:
            yield f
        finally:
            f.close()
    
    def upload_file(self, local_file_path, target_name):
        """Standardized method to move a file into storage."""
        destination = self.base_path / target_name
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(local_file_path, destination)
        print(f"✅ Saved to local: {destination}")

    def save_stream(self, stream, target_name, content_type=None):
        full_path = self.base_path / target_name
        full_path.parent.mkdir(parents=True, exist_ok=True)
        # Local files need to be written in chunks from the stream
        with open(full_path, 'wb') as f:
            shutil.copyfileobj(stream, f)

    def exists(self, target_name):
        return (self.base_path / target_name).exists()