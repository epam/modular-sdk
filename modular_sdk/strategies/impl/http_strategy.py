import requests
from requests import Response

from modular_sdk.commons import ModularException
from modular_sdk.commons.log_helper import get_logger
from modular_sdk.strategies.abstract_strategy import AbstractStrategy

_LOG = get_logger(__name__)


class HTTPStrategy(AbstractStrategy):
    def __init__(
            self,
            sdk_access_key: str,
            sdk_secret_key: str,
            maestro_user: str,
            api_link: str,
    ):
        super().__init__(
            access_key=sdk_access_key,
            secret_key=sdk_secret_key,
            user=maestro_user,
        )
        self.api_link = api_link

    @property
    def http(self):
        return True

    @staticmethod
    def _verify_response(response: Response) -> bytes:
        status_code = response.status_code or 0
        reason = getattr(response.raw, 'reason', '') or 'No specific reason provided'

        def get_json_message(default):
            try:
                return response.json().get('message', default)
            except ValueError:  # Includes JSONDecodeError
                return default

        error_messages = {
            0: f'Empty response received.{reason}',
            401: 'You have been provided Bad Credentials, or resource is not found',
            404: f'Requested resource not found, {reason}',
            413: get_json_message('Payload Too Large'),
            500: f'Error during executing request.{reason}',
        }
        if status_code == 200:
            return response.content

        raise ModularException(
            code=status_code or 204,
            content=error_messages.get(status_code, f'Message: {response.text}'),
        )

    def publish(
            self,
            request_id: str,
            message: bytes,
            headers: dict,
    ) -> bytes:
        response = requests.post(
            url=self.api_link, headers=headers, data=message.decode('utf-8'),
        )
        return self._verify_response(response)
