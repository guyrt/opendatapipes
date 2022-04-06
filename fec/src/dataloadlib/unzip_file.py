from distutils.command.upload import upload
from io import BytesIO
import zipfile

import os
from azure.storage.blob import BlobServiceClient

upload_container = 'rawunzips'


def unzip_and_upload(unzip_request_source):
    """Download file, unzip to memory, and upload containing files to azure blob storage."""
    blob_connection_string = os.environ['FecDataStorageConnectionAppSetting']
    service_client = BlobServiceClient.from_connection_string(blob_connection_string)
    bc = service_client.get_blob_client(container = 'rawzips', blob=unzip_request_source)
    unzip_request_root = unzip_request_source.split('/')[:-1]

    zip_file_contents = bc.download_blob().read_all()
    zf = zipfile.ZipFile(BytesIO(zip_file_contents))
    created_files = []
    total_bytes = 0
    for file in zf.filelist:
        local_file_name = file.filename
        total_bytes += file.file_size

        created_files.append(f"{unzip_request_root}/{local_file_name}")
        contents = zf.read(local_file_name)

        upload_client = service_client.get_blob_client(container=upload_container, blob=local_file_name)
        upload_client.upload(contents)

    return created_files, total_bytes
