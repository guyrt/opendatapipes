import logging
import json

import azure.functions as func

from dataloadlib.unzip_file import unzip_and_upload


def main(msg: func.QueueMessage, outputQueue: func.Out[func.QueueMessage]) -> None:
    """Unzip a file and write all included files to blob storage."""

    queue_message = json.loads(msg.get_body().decode('utf-8'))
    output_messages, total_bytes = unzip_and_upload(queue_message['blobpath'])
    outputQueue.set(tuple(output_messages))

    logging.info(f'UnzipFile ran with {len(output_messages)} files created and {total_bytes} bytes.')
