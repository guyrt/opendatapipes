import logging

import azure.functions as func

from dataloadlib.daily_file_parser import DailyFileWriter, build_parser


def main(msg: func.QueueMessage) -> None:
    file_parser = build_parser()
    uploader = DailyFileWriter(file_parser)

    msg_body = msg.get_body().decode('utf-8')
    logging.info('Processing ProcessFileToKeyValue queue item: %s', msg_body)

    uploader.parse(msg_body)
    