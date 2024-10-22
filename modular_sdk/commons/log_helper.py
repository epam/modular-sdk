import logging
import logging.config

from modular_sdk.commons.constants import Env

logging.config.dictConfig({
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {},
    'handlers': {
        'null_handler': {
            'class': 'logging.NullHandler'
        }
    },
    'loggers': {
        'modular_sdk': {
            'level': Env.LOG_LEVEL.get(),
            'handlers': ['null_handler'],
            'propagate': False,
        },
    }
})


def get_logger(name: str, level: str | None = None, /):
    log = logging.getLogger(name)
    if level:
        log.setLevel(level)
    return log

