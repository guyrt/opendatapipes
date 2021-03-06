import datetime
import logging

from dataloadlib.write_urls_to_process import write_daily

import azure.functions as func

def main(mytimer: func.TimerRequest, outputQueue: func.Out[func.QueueMessage]) -> None:
    """Timer request to build requests. Populates a queue with URL patterns in FEC bulk data capture to check."""

    #
    # Connect to KV and get queue to connect to
    # Push urls to check to the queue
    #

    utc_timestamp = datetime.datetime.utcnow() - datetime.timedelta(days=1)
    utc_str = utc_timestamp.strftime('%Y%m%d')
    messages = write_daily(utc_str)

    outputQueue.set(tuple(messages))

    logging.info(f'PopulateFetchQueue ran on {utc_str} at {utc_timestamp} with {len(messages)} messages')
    logging.info(messages)
