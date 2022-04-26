from os import listdir
from os.path import isfile, join, getsize
import tempfile
import zipfile
from .blob_helpers import get_blob_client, get_service_client

upload_container = 'rawunzips'


def unzip_and_upload(unzip_request_source):
    """Download file, unzip to memory, and upload containing files to azure blob storage."""
    service_client = get_service_client()
    bc = get_blob_client(service_client, 'rawzips', unzip_request_source)
    
    unzip_request_root = "/".join(unzip_request_source.split('/')[:-1])
    
    zip_file_contents = bc.download_blob().readall()

    # save file to disk
    zip_fp = tempfile.TemporaryFile()
    zip_fp.write(zip_file_contents)
    zip_fp.flush()
    zip_fp.seek(0)
    
    unzip_tempdir = tempfile.TemporaryDirectory()
    with zipfile.ZipFile(zip_fp) as zipObj:
        zipObj.extractall(unzip_tempdir.name)

    all_files = [f for f in listdir(unzip_tempdir.name) if isfile(join(unzip_tempdir.name, f))]
    non_files = [f for f in listdir(unzip_tempdir.name) if not isfile(join(unzip_tempdir.name, f))]
    if non_files:
        raise Exception(f"Found directories which is not supported: {', '.join(non_files)}")

    created_files = []
    total_bytes = 0
    for rawfilename in all_files:
        filename = join(unzip_tempdir.name, rawfilename)
        fh = open(filename, 'rb')
        remote_file_name = f"{unzip_request_root}/{rawfilename}"
        total_bytes += getsize(filename)

        created_files.append(remote_file_name)

        upload_client = service_client.get_blob_client(container=upload_container, blob=remote_file_name)
        upload_client.upload_blob(fh, overwrite=True)
        fh.close()

    return created_files, total_bytes


if __name__ == "__main__":
    print(unzip_and_upload("electronic/20220321.zip"))
