import json
from typing import List

import orjson as orjson
from google.cloud import storage

from src.common.gcs.constants import credentials_path, bucket_name, blob_name


def load_json_from_gcs() -> List[dict]:
    """
    Load JSON data from Google Cloud Storage.
    Returns:
        list: List of JSON objects loaded from the specified GCS blob.
    """
    print("Loading JSON from Google Cloud Storage")
    storage_client = storage.Client.from_service_account_json(credentials_path)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    try:
        content = blob.download_as_text()
        return [orjson.loads(line) for line in content.splitlines()]
    except Exception as e:
        print(f"Error processing the file: {e}")
