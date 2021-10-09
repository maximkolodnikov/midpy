import logging.config


log_config = {
    'version': 1,
    'handlers': {
        'console': {
            'level': 'INFO',
            'class': 'logging.StreamHandler',
            'formatter': 'default',
            'stream': 'ext://sys.stdout'
        },
        'log_file': {
            'level': 'INFO',
            'class': 'logging.handlers.RotatingFileHandler',
            'formatter': 'default',
            'filename': 'info.log',
            'maxBytes': 1048576,
            'backupCount': 0
        },
    },
    'loggers': {
        'default': {
            'level': 'DEBUG',
            'handlers': ['log_file', 'console'],
        }
    },
    'formatters': {
        'default': {
            'format': '%(asctime)s - %(levelname)s - %(message)s',
            'datefmt': '%Y-%m-%d %H:%M:%S'
        }
    },
    'disable_existing_loggers': False
}

logging.config.dictConfig(log_config)
logger = logging.getLogger('default')
