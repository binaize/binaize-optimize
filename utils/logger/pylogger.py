import logging.config

from config import *


def get_logger(logger_name, log_level):
    log_path = LOGGING_FOLDER + "/" + logger_name + ".log"
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
                'filename': log_path,
                'mode': 'a',
                'maxBytes': LOGGING_MAX_FILE_SIZE_BYTES,
                'backupCount': LOGGING_LOCAL_BACK_UP_COUNT
            },
            's3': {
                'level': log_level,
                'class': 'aws_logging_handlers.S3.S3Handler',
                'formatter': logger_name,
                'key': logger_name,
                "aws_access_key_id": AWS_ACCESS_KEY_ID,
                "aws_secret_access_key": AWS_SECRET_ACCESS_KEY,
                "bucket": "binaize-dev",
                "max_file_size_bytes": LOGGING_MAX_FILE_SIZE_BYTES,
            }
        },
        'loggers': {
            logger_name: {
                'level': log_level,
                'handlers': ['console', 'file', 's3']
            }
        },
        'disable_existing_loggers': False
    })
    return logging.getLogger(logger_name)
