from os import listdir
from os.path import isfile, join, getsize
import logging
import tempfile
import zipfile
import queue
from threading import Thread

try:
    from .blob_helpers import get_blob_client, get_service_client
except ImportError:
    from blob_helpers import get_blob_client, get_service_client

upload_container = 'rawunzips'

num_parallel_uploads = 10
upload_queue = queue.Queue(maxsize=num_parallel_uploads)
queue_outputs = []
total_queue_bytes = []


def unzip_and_upload(unzip_request):
    """Download file, unzip to memory, and upload containing files to azure blob storage."""
    logging.info("starting unzip")
    unzip_request_source = unzip_request['blobpath']
    datepattern = unzip_request['datepattern']
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

    # in parallel upload

    for i in range(num_parallel_uploads):
        Thread(target = upload_worker, args=(i, ), daemon=True).start()

    for rawfilename in all_files:
        upload_queue.put((rawfilename, unzip_tempdir, unzip_request_root, service_client))

    upload_queue.join()

    created_files = []
    for l in queue_outputs:
        created_files.extend(l)

    queue_messages = [
        {'datepattern': datepattern, 'blobpath': c} for c in created_files
    ]

    return queue_messages, sum(total_queue_bytes)


def upload_worker(thread_num):
    while True:
        item = upload_queue.get()
        print(f"Thread {thread_num} started task.")
        try:
            created_files, total_bytes = upload_file_pool(*item)
            queue_outputs.extend(created_files)
            total_queue_bytes.append(total_bytes)
        except:
            logging.error(f"Upload failed on {item}")

        upload_queue.task_done()


def upload_file_pool(rawfilename, unzip_tempdir, unzip_request_root, service_client):
    logging.info(f"Uploading {rawfilename}")
    print(f'Uploading {rawfilename}')
    created_files = []
    total_bytes = 0
    filename = join(unzip_tempdir.name, rawfilename)
    fh = open(filename, 'rb')
    remote_file_name = f"{unzip_request_root}/{rawfilename}"
    file_size = getsize(filename)
    total_bytes += file_size

    if file_size < 500 * 1000 * 1000: # 500 mb
        created_files.append(remote_file_name)
        upload(service_client, remote_file_name, fh)
    else:
        created_files.extend(split_and_upload(fh, service_client, remote_file_name))

    fh.close()
    return created_files, total_bytes


def split_and_upload(fh, service_client, original_remote_filename):
    """Downstream processes can't handle very large files. Splitting them into 250mb chunks."""
    logging.info(f"Writing split files for {original_remote_filename}.")

    new_files = []
    total_chars = 0
    char_limit = 250000000  # 250m chars ~ 250mb
    file_num = 0
    out_fh = tempfile.NamedTemporaryFile()
    header_line = ""
    for line in fh:
        if not header_line:
            header_line = line
        total_chars += len(line)
        out_fh.write(line)
        if total_chars > char_limit:
            out_fh.flush()
            out_fh.seek(0)
            out_name = _get_filename(original_remote_filename, file_num)
            new_files.append(out_name)
            upload(service_client, out_name, out_fh)

            file_num += 1
            out_fh.close()
            out_fh = tempfile.NamedTemporaryFile()
            out_fh.write(header_line)
            total_chars = 0

    if total_chars > 0:
        out_fh.flush()
        out_fh.seek(0)
        out_name = _get_filename(original_remote_filename, file_num)
        new_files.append(out_name)
        upload(service_client, out_name, out_fh)
        out_fh.close()

    return new_files


def _get_filename(remote_filename, index):
    parts = remote_filename.rsplit('.', 1)
    if len(parts) == 1:
        return f"{parts[0]}_{index}"
    return f"{'.'.join(parts[:-1])}_{index}.{parts[-1]}"


def upload(service_client, remote_filename, fh):
    logging.info(f"Uploading {remote_filename}")
    upload_client = service_client.get_blob_client(container=upload_container, blob=remote_filename)
    upload_client.upload_blob(fh, overwrite=True)


if __name__ == "__main__":
    import sys
    logging.basicConfig(stream=sys.stdout)
    print(unzip_and_upload({'blobpath': "electronic/20220221.zip", 'datepattern': '20220221'}))
