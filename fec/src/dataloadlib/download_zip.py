import json
import requests
import logging

def download_zip(download_request, outputQueue, outputBlob):
    response = requests.get(download_request['pattern'])
    if response.status_code != 200:
        logging.error("Error code {0} on request {1}".format(response.status_code, download_request['pattern']))
        return
    zipfile = response.content
    outputBlob.set(zipfile)
    queue_msg = json.dumps(download_request)
    outputQueue.set(queue_msg)
