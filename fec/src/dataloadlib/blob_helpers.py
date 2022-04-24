import os
from azure.storage.blob import BlobServiceClient, BlobClient


def get_blob_client(service_client : BlobServiceClient, container : str, blob : str) -> BlobClient:
    bc = service_client.get_blob_client(container = container, blob=blob)
    return bc


def get_service_client() -> BlobServiceClient:
    blob_connection_string = os.environ['FecDataStorageConnectionAppSetting']
    service_client = BlobServiceClient.from_connection_string(blob_connection_string)
    return service_client