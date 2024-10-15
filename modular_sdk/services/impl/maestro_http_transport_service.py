import binascii
import json
import urllib.request
import urllib.parse
import urllib.error
from typing import Iterable, Any
from modular_sdk.commons import (
    ModularException, generate_id, build_secure_message, build_message,
)
from modular_sdk.commons.constants import SUCCESS_STATUS
from modular_sdk.commons.log_helper import get_logger
from modular_sdk.services.impl.maestro_signature_builder import (
    MaestroSignatureBuilder,
)

HTTP_DEFAULT_RESPONSE_TIMEOUT = 30
_LOG = get_logger(__name__)


class MaestroHTTPTransport:
    def __init__(
            self,
            sdk_access_key: str,
            sdk_secret_key: str,
            maestro_user: str,
            api_link: str,
            timeout: int = HTTP_DEFAULT_RESPONSE_TIMEOUT,
    ):
        self.access_key = sdk_access_key
        self.secret_key = sdk_secret_key
        self.user = maestro_user
        self.api_link = api_link
        self.timeout = timeout

    def pre_process_request(
            self,
            command_name: str,
            parameters,
            secure_parameters: Iterable | None = None,
            is_flat_request: bool = False,
            async_request: bool = False,
            compressed: bool = False,
    ) -> tuple[bytes, dict]:
        request_id = generate_id()
        _LOG.debug('Going to pre-process HTTP request')
        message = build_message(
            command_name=command_name,
            parameters=parameters,
            request_id=request_id,
            is_flat_request=is_flat_request,
            compressed=compressed,
        )
        secure_message = message
        if not compressed:
            secure_message = build_secure_message(
                command_name=command_name,
                parameters_to_secure=parameters,
                secure_parameters=secure_parameters,
                request_id=request_id,
                is_flat_request=is_flat_request,
            )
        _LOG.debug(
            f'Prepared command: {command_name}\nCommand format: {secure_message}'
        )
        signer = MaestroSignatureBuilder(
            access_key=self.access_key,
            secret_key=self.secret_key,
            user=self.user,
        )
        encrypted_body = signer.encrypt(data=message)

        _LOG.debug('Message encrypted')
        # sign headers
        headers = signer.get_signed_headers(
            async_request=async_request,
            compressed=compressed,
        )
        _LOG.debug('Signed headers prepared')
        return encrypted_body, headers

    def post_process_request(self, response: bytes) -> tuple[int, str, Any]:
        signer = MaestroSignatureBuilder(
            access_key=self.access_key,
            secret_key=self.secret_key,
            user=self.user,
        )
        try:
            response_item = signer.decrypt(data=response)
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
        return status_code, status, response

    @staticmethod
    def _verify_response(response) -> bytes:
        content = response.read()
        status_code = response.getcode()
        reason = response.reason or 'No specific reason provided'
        _LOG.debug(f'Response status code: {status_code}, reason: {reason}')
        if 200 <= status_code < 300:
            _LOG.debug('Successfully received response')
            return content
        _LOG.error(f'Error with status code {status_code}: {content}')
        raise ModularException(code=status_code, content=content)

    def send_sync(self, *args, **kwargs) -> tuple[int, str, Any]:
        message, headers = self.pre_process_request(*args, **kwargs)
        req = urllib.request.Request(
            url=self.api_link, headers=headers, data=message, method='POST',
        )
        with urllib.request.urlopen(req, timeout=self.timeout) as response:
            response_content = self._verify_response(response)
        return self.post_process_request(response=response_content)

    def send_async(self, *args, **kwargs) -> None:
        try:
            message, headers = self.pre_process_request(*args, **kwargs)
            req = urllib.request.Request(
                url=self.api_link, headers=headers, data=message, method='POST',
            )
            with urllib.request.urlopen(req, timeout=self.timeout):
                _LOG.info('Async request sent. No response will be processed')
        except Exception as e:
            _LOG.error(f'Error sending request: {e}')
