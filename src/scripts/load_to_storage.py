import requests
import traceback
from tqdm import tqdm
import argparse
from src.config.manager import ResourceManager # switchboard


def download_file(target_path, url):
    # 1. Get the active storage (Automatic Local vs Cloud)
    storage = ResourceManager.get_main_storage()

    # 2. Check existence using our unified method
    if not storage.exists(target_path):
        print(f"File {target_path} does not exist, downloading...")

        try:
            with requests.get(url, stream=True) as r:
                r.raise_for_status()
                total_size = int(r.headers.get('Content-Length', 0))

                # Use tqdm to wrap the stream for the progress bar
                with tqdm.wrapattr(
                    r.raw, "read",
                    total=total_size,
                    desc=f"Transferring {target_path}"
                ) as buffered_stream:

                    # 3. The Connector handles the specific "Save" logic
                    storage.save_stream(
                        buffered_stream, 
                        target_path, 
                        content_type=r.headers.get('content-type')
                    )
        except Exception as e:
            print(f"❌ Download failed: {e}")
            traceback.print_exc()
            raise e
    else:
        print(f"✅ {target_path} already exists in storage.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download a file to storage.")
    parser.add_argument("target_path", help="The target path in storage where the file will be saved.")
    parser.add_argument("url", help="The URL of the file to download.")

    args = parser.parse_args()
    download_file(args.target_path, args.url)