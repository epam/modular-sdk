import binascii
import json
import os
import requests

from modular_sdk.commons import ModularException
from modular_sdk.commons.constants import SUCCESS_STATUS
from modular_sdk.commons.log_helper import get_logger
from modular_sdk.strategies.abstract_strategy import AbstractStrategy

_LOG = get_logger(__name__)


class HTTPStrategy(AbstractStrategy):
    def __init__(
            self,
            sdk_access_key: str = None,
            sdk_secret_key: str = None,
            maestro_user: str = None,
            api_link: str = None,
    ):
        api_link = api_link or os.getenv('API_LINK')
        if not api_link:
            raise ValueError("Missing 'api_link' parameter")

        super().__init__(
            access_key=sdk_access_key,
            secret_key=sdk_secret_key,
            user=maestro_user,
        )
        self.api_link = api_link

    def _pre_process_request(self, request_data: dict, command_name: str):
        _LOG.debug('Generating request_id')
        request_id = super()._generate_id()
        _LOG.debug('Signing HTTP headers')
        headers = super()._get_signed_headers(
            access_key=self.access_key,
            secret_key=self.secret_key,
            user=self.user,
            async_request=False,
            http=True,
        )
        _LOG.debug('Encrypting HTTP body')
        body = self._build_payload(
            id=request_id,
            command_name=command_name,
            parameters=request_data,
            is_flat_request=False,
        )
        encrypted_body = super()._encrypt(secret_key=self.secret_key, data=body)
        encrypted_body = encrypted_body.decode('utf-8')
        return request_id, headers, encrypted_body

    def post_process_request(self, response):
        try:
            response_item = self._decrypt(
                secret_key=self.secret_key, data=response,
            )
            _LOG.debug('Message from M3-server successfully decrypted')
        except binascii.Error:
            response_item = response.decode('utf-8')
        try:
            _LOG.debug(f'Raw decrypted message from server: {response_item}')
            response_json = json.loads(response_item).get('results')[0]
        except json.decoder.JSONDecodeError:
            _LOG.error('Response cannot be decoded - invalid JSON string')
            raise ModularException(code=502, content="Response can't be decoded")
        status = response_json.get('status')
        status_code = response_json.get('statusCode')
        warnings = response_json.get('warnings')
        if status == SUCCESS_STATUS:
            data = response_json.get('data')
        else:
            data = response_json.get('error') or response_json.get('readableError')
        try:
            data = json.loads(data)
        except json.decoder.JSONDecodeError:
            data = data
        response = {'status': status,'status_code': status_code}
        if isinstance(data, str):
            response.update({'message': data})
        if isinstance(data, dict):
            data = [data]
        if isinstance(data, list):
            response.update({'items': data})
        if items := response_json.get('items'):
            response.update({'items': items})
        if table_title := response_json.get('tableTitle'):
            response.update({'table_title': table_title})
        if warnings:
            response.update({'warnings': warnings})
        return response

    @staticmethod
    def _verify_response(response):
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
            return response.content.decode()

        raise ModularException(
            code=status_code or 204,
            content=error_messages.get(status_code, f'Message: {response.text}'),
        )

    def execute(
            self,
            command_name: str,
            request_data: dict,
            **kwargs,
    ):
        request_id, headers, encrypted_body = self._pre_process_request(
            request_data=request_data, command_name=command_name,
        )
        _LOG.debug(
            f'Going to send post request to maestro server with '
            f'request_id: {request_id}, encrypted_body: {encrypted_body}'
        )
        response = requests.post(
            url=self.api_link, headers=headers, data=encrypted_body,
        )
        _LOG.debug('Going to verify and process server response')
        response = self._verify_response(response)
        return self.post_process_request(response=response)
