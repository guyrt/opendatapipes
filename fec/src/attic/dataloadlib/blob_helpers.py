import os
from azure.storage.blob import BlobServiceClient, BlobClient


def get_blob_client(service_client : BlobServiceClient, container : str, blob : str) -> BlobClient:
    bc = service_client.get_blob_client(container = container, blob=blob)
    if bc.exists():
        print(f"Blob client for {blob} sized {bc.get_blob_properties()['size'] / 1024 / 1024} mb.")
    return bc


def get_service_client(blob_connection_string=None) -> BlobServiceClient:
    if blob_connection_string is None:
        blob_connection_string = os.environ['FecDataStorageConnectionAppSetting']
    service_client = BlobServiceClient.from_connection_string(blob_connection_string)
    return service_client
