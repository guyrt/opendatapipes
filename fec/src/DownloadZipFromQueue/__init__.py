import logging
import json

import azure.functions as func
from dataloadlib.download_zip import download_zip


def main(msg: func.QueueMessage, outputQueue: func.Out[func.QueueMessage], outputblob: func.Out[bytes]) -> None:
    msg_body = msg.get_body().decode('utf-8')
    logging.info('Processing queue item: %s', msg_body)

    msg_json = json.loads(msg_body)
    download_zip(msg_json, outputQueue, outputblob)
