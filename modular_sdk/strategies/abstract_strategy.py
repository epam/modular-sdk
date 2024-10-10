import base64
import binascii
import gzip
import hashlib
import hmac
import json
import os
from typing import Iterable, Any
from abc import ABC, abstractmethod
from datetime import datetime
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

from modular_sdk.commons import ModularException, generate_id
from modular_sdk.commons.constants import SUCCESS_STATUS
from modular_sdk.commons.log_helper import get_logger

_LOG = get_logger(__name__)


class AbstractStrategy(ABC):
    def __init__(self, access_key: str, secret_key: str, user: str):
        self.access_key = access_key
        self.secret_key = secret_key
        self.user = user

    @property
    def http(self):
        return False

    @abstractmethod
    def publish(
            self,
            request_id: str,
            message: bytes,
            headers: dict,
    ) -> bytes:
        pass

    def execute(
            self,
            command_name: str,
            request_data: dict,
            secure_parameters: Iterable | None = None,
            is_flat_request: bool = False,
            compressed: bool = False,
            **kwargs,
    ):
        request_id, headers, encrypted_body = self._pre_process_request(
            command_name=command_name,
            parameters=request_data,
            secure_parameters=secure_parameters,
            is_flat_request=is_flat_request,
            async_request=False,
            compressed=compressed,
            http=self.http,
        )
        _LOG.debug(
            f'Going to send post request to maestro server with '
            f'request_id: {request_id}, encrypted_body: {encrypted_body}'
        )
        response = self.publish(
            request_id=request_id, message=encrypted_body, headers=headers,
        )
        _LOG.debug('Going to verify and process server response')
        return self._post_process_request(response=response)

    @staticmethod
    def _get_signed_headers(
            access_key,
            secret_key: str,
            user: str,
            async_request: bool = False,
            compressed: bool = False,
            http: bool = False,
    ) -> dict:
        """
        Create and sign necessary headers for interaction with Maestro API
        """
        if not access_key or not user:
            raise ModularException(
                code=503,
                content='Cannot detect access_key or user. Please add it first'
            )
        date = int(datetime.now().timestamp()) * 1000
        signature = hmac.new(
            key=bytearray(f'{secret_key}{date}'.encode('utf-8')),
            msg=bytearray(f'M3-POST:{access_key}:{date}:{user}'.encode('utf-8')),
            digestmod=hashlib.sha256,
        ).hexdigest()
        n = 2
        resolved_signature = ''
        for each in [signature[i:i + n] for i in range(0, len(signature), n)]:
            resolved_signature += '1' + each
        headers = {
            "maestro-authentication": resolved_signature,
            "maestro-request-identifier": "api-server",
            "maestro-user-identifier": user,
            "maestro-date": str(date),
            "maestro-accesskey": str(access_key),
            "maestro-sdk-version": "3.2.80",
            "maestro-sdk-async": 'true' if async_request else 'false',
            "compressed": str(compressed).lower() if http else compressed,
        }
        return headers

    @staticmethod
    def _encrypt(secret_key: str, data: Any) -> bytes:
        """
        Encrypt data, add initialization vector ("iv") at beginning of encrypted
        message and encode entire data in Base64 format
        """
        if not secret_key:
            raise ModularException(
                code=503, content="Can't detect secret_key. Please add it first"
            )
        iv = os.urandom(12)
        plain_text = data if isinstance(data, str) else json.dumps(data)
        data_in_bytes = plain_text.encode('utf-8')
        try:
            cipher = AESGCM(key=secret_key.encode('utf-8'))
        except ValueError as e:
            raise ValueError(str(e).replace('AESGCM key', 'Secret Key'))
        encrypted_data = cipher.encrypt(
            nonce=iv, data=data_in_bytes, associated_data=None,
        )
        encrypted_data_with_iv = bytes(iv) + encrypted_data
        base64_request = base64.b64encode(encrypted_data_with_iv)
        return base64_request

    @staticmethod
    def _decrypt(secret_key: str, data: str | bytes) -> bytes:
        """
        Decode received message from Base64 format, cut initialization
        vector ("iv") from beginning of the message, decrypt message
        """
        decoded_data = base64.b64decode(data)
        iv = decoded_data[:12]
        encrypted_data = decoded_data[12:]
        cipher = Cipher(
            algorithms.AES(key=secret_key.encode('utf-8')),
            modes.GCM(initialization_vector=iv),
        ).decryptor()
        origin_data_with_iv = cipher.update(encrypted_data)
        # Due to Initialization vector in encrypting method
        # there is need to split useful and useless parts of the
        # server response.
        response = origin_data_with_iv[:-16]
        return response

    @staticmethod
    def _build_payload(
            request_id: str,
            command_name: str,
            parameters: dict,
            is_flat_request: bool,
    ) -> list[dict]:
        if is_flat_request:
            parameters.update({'type': command_name})
            result = [{'id': request_id, 'type': None, 'params': parameters}]
        else:
            result = [
                {'id': request_id, 'type': command_name,'params': parameters}
            ]
        return result

    def _build_message(
            self,
            request_id: str,
            command_name: str,
            parameters: list[dict] | dict,
            is_flat_request: bool = False,
            compressed: bool = False,
    ) -> list[dict] | str:
        if isinstance(parameters, list):
            result = []
            for payload in parameters:
                result.extend(
                    self._build_payload(
                        request_id, command_name, payload, is_flat_request,
                    )
                )
        else:
            result = self._build_payload(
                request_id, command_name, parameters, is_flat_request,
            )
        if compressed:
            return base64.b64encode(
                gzip.compress(json.dumps(result).encode('UTF-8'))
            ).decode()
        return result

    def _build_secure_message(
            self,
            request_id: str,
            command_name: str,
            parameters_to_secure: dict,
            secure_parameters: Iterable | None = None,
            is_flat_request: bool = False,
    ) -> list[dict] | str:
        if not secure_parameters:
            secure_parameters = []
        secured_parameters = {
            k: (v if k not in secure_parameters else '*****')
            for k, v in parameters_to_secure.items()
        }
        return self._build_message(
            request_id=request_id,
            command_name=command_name,
            parameters=secured_parameters,
            is_flat_request=is_flat_request,
        )

    def _pre_process_request(
            self,
            command_name: str,
            parameters,
            secure_parameters: Iterable | None = None,
            is_flat_request: bool = False,
            async_request: bool = False,
            compressed: bool = False,
            http: bool = False,
    ) -> tuple:
        _LOG.debug('Generating request_id')
        request_id = generate_id()
        _LOG.debug('Signing HTTP headers')
        headers = self._get_signed_headers(
            access_key=self.access_key,
            secret_key=self.secret_key,
            user=self.user,
            async_request=async_request,
            compressed=compressed,
            http=http,
        )
        _LOG.debug('Going to pre-process request')
        message = self._build_message(
            command_name=command_name,
            parameters=parameters,
            request_id=request_id,
            is_flat_request=is_flat_request,
            compressed=compressed,
        )
        secure_message = message
        if not compressed:
            secure_message = self._build_secure_message(
                command_name=command_name,
                parameters_to_secure=parameters,
                secure_parameters=secure_parameters,
                request_id=request_id,
                is_flat_request=is_flat_request
            )
        _LOG.debug(
            f'Prepared command: {command_name}\nCommand format: {secure_message}'
        )
        encrypted_body = self._encrypt(secret_key=self.secret_key, data=message)
        _LOG.debug('Message encrypted')
        return request_id, headers, encrypted_body

    def _post_process_request(self, response: str | bytes):
        try:
            response_item = self._decrypt(
                secret_key=self.secret_key,
                data=response,
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
