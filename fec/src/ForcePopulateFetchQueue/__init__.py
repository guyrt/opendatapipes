import logging

import azure.functions as func

from dataloadlib.write_urls_to_process import write_annual, write_daily, write_daily_range


def main(req: func.HttpRequest, outputQueue: func.Out[func.QueueMessage]) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    req_body = req.get_json()
    messages = []
    for daily_key in req_body.get('daily', []):
        messages.extend(write_daily(daily_key))

    for annual_key in req_body.get('annual_keys', []):
        messages.extend(write_annual(annual_key))

    if req_body.get('range'):
        start, end = req_body.get('range')
        messages.extend(write_daily_range(start, end))

    outputQueue.set(tuple(messages))

    out_message = "Success on {0} messages\n\n{1}".format(len(messages), messages)

    return func.HttpResponse(
            out_message,
            status_code=200
    )
