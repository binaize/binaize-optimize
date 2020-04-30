import logging.config

from config import *
import os


def get_log_path(name):
    return LOG_FOLDER + "/" + name + ".log"


def configure_logger(logger_name, log_path, log_level):
    directory = os.path.dirname(log_path)
    if not os.path.exists(directory):
        os.makedirs(directory)
    logging.config.dictConfig({
        'version': 1,
        'formatters': {
            logger_name: {'format': '%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s',
                          'datefmt': '%Y-%m-%d %H:%M:%S'}
        },
        'handlers': {
            'console': {
                'level': log_level,
                'class': 'logging.StreamHandler',
                'formatter': logger_name,
                'stream': 'ext://sys.stdout'
            },
            'file': {
                'level': log_level,
                'class': 'logging.handlers.RotatingFileHandler',
                'formatter': logger_name,
                'filename': log_path
            }
        },
        'loggers': {
            logger_name: {
                'level': log_level,
                'handlers': ['console', 'file']
            }
        },
        'disable_existing_loggers': False
    })
    return logging.getLogger(logger_name)
