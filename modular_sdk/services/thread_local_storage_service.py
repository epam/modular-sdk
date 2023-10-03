from threading import local

from modular_sdk.commons.log_helper import get_logger

_LOG = get_logger(__name__)


class ThreadLocalStorageService:
    def __init__(self):
        self.__storage = None

    @property
    def storage(self):
        if not self.__storage:
            self.__storage = local()
            self.__storage.value = {}
        return self.__storage.value

    def set(self, key: str, value):
        _LOG.debug(f'Setting {key}:{value} to storage')
        self.storage[key] = value

    def get(self, key):
        _LOG.debug(f'Extracting {key} var from storage')
        return self.storage.get(key)

    def pop(self, key):
        _LOG.debug(f'Pop {key} var from storage')
        return self.storage.pop(key, None)
