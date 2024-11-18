"""
Script Name: dl_adls.py
Arguments: None
Description: Datalake ADLS (Azure Data Lake Storage) module
Created: May 22, 2024
Author: Sudheer B
Version: 1.0
"""

from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import io
import os
import pandas as pd
from .dl_logger import DLLogger

dl_log = DLLogger(__name__)

class DLAzureDataLake:
    def __init__(self, connection_string=None, container_name=None):
        """Initialize Azure Data Lake connection"""
        self.connection_string = connection_string or os.environ.get('AZURE_STORAGE_CONNECTION_STRING')
        if not self.connection_string:
            raise ValueError("AZURE_STORAGE_CONNECTION_STRING not found in environment variables")
        
        self.container_name = container_name
        self.blob_service_client = BlobServiceClient.from_connection_string(self.connection_string)
        self.container_client = self._get_container_client()

    def _get_container_client(self):
        """Get or create container client"""
        if not self.container_name:
            raise ValueError("Container name must be specified")
            
        container_client = self.blob_service_client.get_container_client(self.container_name)
        if not container_client.exists():
            dl_log.info(f"Creating container: {self.container_name}")
            self.blob_service_client.create_container(self.container_name)
            container_client = self.blob_service_client.get_container_client(self.container_name)
        return container_client

    def upload_file(self, local_file_path, blob_name):
        """Upload a local file to Azure Data Lake"""
        if not os.path.exists(local_file_path):
            raise FileNotFoundError(f"Local file not found: {local_file_path}")

        blob_client = self.container_client.get_blob_client(blob_name)
        with open(local_file_path, "rb") as data:
            blob_client.upload_blob(data, overwrite=True)
        dl_log.info(f"Uploaded {blob_name} to container {self.container_name}")

    def download_file(self, blob_name, local_file_path):
        """Download a file from Azure Data Lake"""
        blob_client = self.container_client.get_blob_client(blob_name)
        with open(local_file_path, "wb") as download_file:
            download_file.write(blob_client.download_blob().readall())
        dl_log.info(f"Downloaded {blob_name} to {local_file_path}")

    def list_files(self, prefix=None):
        """List files in the container with optional prefix"""
        blobs = self.container_client.list_blobs(name_starts_with=prefix)
        blob_names = [blob.name for blob in blobs]
        for blob_name in blob_names:
            dl_log.info(f"Found: {blob_name}")
        return blob_names

    def delete_file(self, blob_name):
        """Delete a file from Azure Data Lake"""
        blob_client = self.container_client.get_blob_client(blob_name)
        blob_client.delete_blob()
        dl_log.info(f"Deleted {blob_name} from container {self.container_name}")

    def upload_dataframe(self, df, blob_name, file_format='csv', **kwargs):
        """Upload a pandas or snowpark DataFrame to Azure Data Lake"""
        if isinstance(df, DataFrame):
            df = df.to_pandas()
        
        buffer = io.BytesIO()
        if file_format.lower() == 'csv':
            df.to_csv(buffer, index=False, **kwargs)
        elif file_format.lower() == 'parquet':
            df.to_parquet(buffer, index=False, **kwargs)
        else:
            raise ValueError(f"Unsupported file format: {file_format}")

        buffer.seek(0)
        blob_client = self.container_client.get_blob_client(blob_name)
        blob_client.upload_blob(buffer.getvalue(), overwrite=True)
        dl_log.info(f"Uploaded DataFrame as {file_format} to {blob_name}")

    def read_dataframe(self, blob_name, file_format='csv', **kwargs):
        """Read a file from Azure Data Lake into a pandas DataFrame"""
        blob_client = self.container_client.get_blob_client(blob_name)
        download_stream = blob_client.download_blob()
        
        if file_format.lower() == 'csv':
            return pd.read_csv(io.BytesIO(download_stream.readall()), **kwargs)
        elif file_format.lower() == 'parquet':
            return pd.read_parquet(io.BytesIO(download_stream.readall()), **kwargs)
        else:
            raise ValueError(f"Unsupported file format: {file_format}")
