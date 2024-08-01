from datetime import datetime
from typing import Union
from google.oauth2 import service_account
from google.cloud import storage
import pandas as pd

class StorageConnector:

    def __init__(self, key_path: str, scopes: Union[list, tuple]):
        credentials = service_account.Credentials.from_service_account_file(key_path, scopes=scopes)
        self.storage_client = storage.Client(credentials=credentials)

    def upload_to_storage(self, bucket: str, blob_name: str, upload_data: str) -> None:
        bucket = self.storage_client.bucket(bucket)
        blob = bucket.blob(blob_name)
        if blob.exists():
            blob.delete()
        blob.upload_from_string(upload_data)

    def download_from_storage(self, bucket: str, blob_name: str) -> str:
        bucket = self.storage_client.bucket(bucket)
        blob = bucket.blob(blob_name)
        if not blob.exists():
            raise FileNotFoundError(f"The blob {blob_name} does not exist in bucket {bucket}.")
        return blob.download_as_text()

    @staticmethod
    def add_creation_log_to_df(df: pd.DataFrame) -> pd.DataFrame:
        now = datetime.now()
        df['created_at'] = now
        df['updated_at'] = now
        return df

