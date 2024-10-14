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

    @abstractmethod
    def pre_process_request(self, *args, **kwargs) -> tuple[str | bytes, dict]:
        # signing, encypt
        pass

    @abstractmethod
    def post_process_request(self, *args, **kwargs) -> tuple[int, str, str]:
        # sign check, decrypt
        pass

    def __resolve_http_option(self, api_link) -> str:
        api_link = api_link or self.api_link
        return api_link

    @staticmethod
    def _verify_response(response) -> bytes:
        status_code = response.getcode()
        reason = response.reason or 'No specific reason provided'

        def get_json_message(default):
            try:
                response_json = json.loads(response.read().decode())
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
            return response.read()

        error_message = error_messages.get(
            status_code, f'Message: {response.read().decode()}'
        )
        _LOG.error(f'Error with status code {status_code}: {error_message}')
        raise ModularException(code=status_code or 204, content=error_message)

    def send_sync(self, *args, **kwargs) -> tuple[int, str, Any]:
        message, headers = self.pre_process_request(*args, **kwargs)
        http_config = kwargs.get('config')
        api_link = self.__resolve_http_option(
            api_link=http_config.api_link if http_config else None,
        )
        req = urllib.request.Request(
            url=api_link,
            headers=headers,
            data=message,
            # data=message.decode('utf-8').encode('utf-8'),
            method='POST',
        )
        response = urllib.request.urlopen(req, timeout=http_config.timeout)
        # TODO: investigate it, to tests
        if not response:
            raise ModularException(
                code=502,
                content=f'Response was not received. '
                        f'Timeout: {http_config.timeout} seconds.'
            )
        response_item = self._verify_response(response)
        return self.post_process_request(response=response_item)

    # def send_async(self, *args, **kwargs) -> None:
    #     message, headers = self.pre_process_request(*args, **kwargs)
    #     http_config = kwargs.get('config')
    #     request_queue, exchange, response_queue = \
    #         self.__resolve_http_option(
    #             api_link=http_config.api_link if http_config else None,
    #         )
    #
    #     return self.rabbit.publish(
    #         routing_key=request_queue,
    #         exchange=exchange,
    #         message=message,
    #         headers=headers,
    #         content_type=PLAIN_CONTENT_TYPE)
