import json
import urllib.request
from abc import abstractmethod
from typing import Any

from modular_sdk.commons import ModularException
from modular_sdk.commons.log_helper import get_logger

HTTP_DEFAULT_RESPONSE_TIMEOUT = 30
_LOG = get_logger(__name__)


class HTTPConfig:
    def __init__(
            self,
            api_link: str,
            timeout: int = HTTP_DEFAULT_RESPONSE_TIMEOUT,
    ):
        self.api_link = api_link
        self.timeout = timeout


class HTTPTransport:
    def __init__(self, config: HTTPConfig):
        self.api_link = config.api_link
        self.timeout = config.timeout

    @abstractmethod
    def pre_process_request(self, *args, **kwargs) -> tuple[str | bytes, dict]:
        # signing, encypt
        pass

    @abstractmethod
    def post_process_request(self, *args, **kwargs) -> tuple[int, str, str]:
        # sign check, decrypt
        pass

    @staticmethod
    def _verify_response(response) -> bytes:
        content = response.read()
        status_code = response.getcode()
        reason = response.reason or 'No specific reason provided'

        def get_json_message(default):
            try:
                response_json = json.loads(content.decode())
                return response_json.get('message', default)
            except ValueError:  # Includes JSONDecodeError
                return default

        error_messages = {
            0: f'Empty response received.{reason}',
            401: 'You have been provided Bad Credentials, or resource is not found',
            404: f'Requested resource not found, {reason}',
            413: get_json_message('Payload Too Large'),
            500: f'Error during executing request.{reason}',
        }
        _LOG.debug(f'Response status code: {status_code}, reason: {reason}')
        if status_code == 200:
            _LOG.debug('Successfully received response')
            return content

        error_message = error_messages.get(
            status_code, f'Message: {content.decode()}'
        )
        _LOG.error(f'Error with status code {status_code}: {error_message}')
        raise ModularException(code=status_code or 204, content=error_message)

    def send_sync(self, *args, **kwargs) -> tuple[int, str, Any]:
        message, headers = self.pre_process_request(*args, **kwargs)
        req = urllib.request.Request(
            url=self.api_link,
            headers=headers,
            data=message,
            method='POST',
        )
        response = urllib.request.urlopen(req, timeout=self.timeout)
        if not response:
            raise ModularException(
                code=502,
                content=f'Response was not received. '
                        f'Timeout: {self.timeout} seconds.'
            )
        response_item = self._verify_response(response)
        return self.post_process_request(response=response_item)
