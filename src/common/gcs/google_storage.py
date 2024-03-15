import json
from typing import List

import orjson as orjson
from google.cloud import storage

from src.common.gcs.constants import gcs_credentials_path, gcs_bucket_name, gcs_blob_name, json_file_local_path


def load_json_from_gcs() -> List[dict]:
    """
    Load JSON data from Google Cloud Storage.
    Returns:
        list: List of JSON objects loaded from the specified GCS blob.
    """
    print("Loading JSON from Google Cloud Storage")
    storage_client = storage.Client.from_service_account_json(gcs_credentials_path)
    bucket = storage_client.bucket(gcs_bucket_name)
    blob = bucket.blob(gcs_blob_name)

    try:
        content = blob.download_as_text()
        return [orjson.loads(line) for line in content.splitlines()]
    except Exception as e:
        print(f"Error processing the file: {e}")


def load_json_from_local() -> List[dict]:
    """
    Load JSON data from local file.
    Returns:
        list: List of JSON objects loaded from the specified local file.
    """
    gcp_file = []
    with open(json_file_local_path, 'r') as file:
        for line in file:
            gcp_file.append(json.loads(line))
    return gcp_file
