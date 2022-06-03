import logging
import sys


log_level = logging.INFO
loggers = []
FORMATTER = logging.Formatter('%(asctime)s - %(levelname)s - %(module)s - %(message)s')
STREAM_HANDLER = logging.StreamHandler(sys.stderr)
STREAM_HANDLER.setLevel(log_level)
STREAM_HANDLER.setFormatter(FORMATTER)
FILE_HANDLER = logging.FileHandler('debug.log')
FILE_HANDLER.setLevel(logging.DEBUG)
FILE_HANDLER.setFormatter(FORMATTER)


def change_log_level(level):
    STREAM_HANDLER.setLevel(level)
    for logger in loggers:
        logger.setLevel(level)


def configure_logger():
    log = logging.getLogger(__name__)
    log.addHandler(STREAM_HANDLER)
    log.addHandler(FILE_HANDLER)
    log.setLevel(log_level)
    loggers.append(log)
    return log