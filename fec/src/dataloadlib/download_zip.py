import json
import tempfile
import urllib.request

from .blob_helpers import get_blob_client, get_service_client


def download_zip(download_request):
    request_url = download_request['pattern']
    tmpfile_name = tempfile.NamedTemporaryFile()
    urllib.request.urlretrieve(request_url, tmpfile_name.name)
    tmpfile_name.seek(0)

    # upload blob
    service_client = get_service_client()
    out_blob = get_blob_client(service_client, "rawzips", download_request['blobpath'])
    out_blob.upload_blob(tmpfile_name, overwrite=True)

    queue_msg = json.dumps(download_request)
    return queue_msg
