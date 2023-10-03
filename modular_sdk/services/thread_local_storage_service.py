from threading import local

from modular_sdk.commons.log_helper import get_logger

_LOG = get_logger(__name__)


class ThreadLocalStorageService:
    def __init__(self):
        self.__storage = local()
        self.__storage.value = {}

    def set(self, key: str, value):
        _LOG.debug(f'Setting {key}:{value} to storage')
        self.__storage.value[key] = value

    def get(self, key):
        _LOG.debug(f'Extracting {key} var from storage')
        return self.__storage.value.get(key)

    def pop(self, key):
        _LOG.debug(f'Pop {key} var from storage')
        return self.__storage.value.pop(key, None)
