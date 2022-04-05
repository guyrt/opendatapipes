import logging

import azure.functions as func

from dataloadlib.write_urls_to_process import write_annual, write_daily


def main(req: func.HttpRequest, outputQueue: func.Out[func.QueueMessage]) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    req_body = req.get_json()
    for daily_key in req_body.get('daily'):
        write_daily(daily_key, outputQueue, req_body.get('force', False))

    for annual_key in req_body.get('annual_keys'):
        write_annual(annual_key, outputQueue)

    return func.HttpResponse(
            "Success",
            status_code=200
    )
