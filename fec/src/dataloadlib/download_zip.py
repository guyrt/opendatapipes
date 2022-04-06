import json
import requests

def download_zip(download_request, outputQueue, outputBlob):
    zipfile = requests.get(download_request['pattern']).content
    outputBlob.set(zipfile)
    queue_msg = json.dumps(download_request)
    outputQueue.set(queue_msg)
