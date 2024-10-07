import base64
import gzip
import hashlib
import hmac
import json
import os
import uuid
from abc import ABC, abstractmethod
from datetime import datetime
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

from modular_sdk.commons import ModularException
from modular_sdk.commons.log_helper import get_logger

_LOG = get_logger(__name__)


class AbstractStrategy(ABC):
    def __init__(self, access_key: str, secret_key: str, user: str):
        self.access_key = access_key
        self.secret_key = secret_key
        self.user = user

    @abstractmethod
    def post_process_request(self, response):
        pass

    @abstractmethod
    def execute(
            self,
            command_name: str,
            request_data: dict,
            secure_parameters,
            is_flat_request: bool,
            **kwargs,
    ):
        pass

    @staticmethod
    def _generate_id() -> str:
        return str(uuid.uuid4())

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
    def _encrypt(secret_key: str, data) -> bytes:
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
    def _decrypt(secret_key: str, data) -> bytes:
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
        response = origin_data_with_iv[:-16]
        return response

    @staticmethod
    def _build_payload(
            id: str,
            command_name: str,
            parameters: dict,
            is_flat_request: bool,
    ) -> list:
        if is_flat_request:
            parameters.update({'type': command_name})
            result = [{'id': id, 'type': None, 'params': parameters}]
        else:
            result = [{'id': id, 'type': command_name,'params': parameters}]
        return result

    def _build_message(
            self,
            id: str,
            command_name: str,
            parameters: dict,
            is_flat_request: bool = False,
            compressed: bool = False,
    ) -> list | str:
        if isinstance(parameters, list):
            result = []
            for payload in parameters:
                result.extend(
                    self._build_payload(id, command_name, payload, is_flat_request)
                )
        else:
            result = self \
                ._build_payload(id, command_name, parameters, is_flat_request)
        if compressed:
            return base64.b64encode(
                gzip.compress(json.dumps(result).encode('UTF-8'))
            ).decode()
        return result

    def _build_secure_message(
            self,
            id: str,
            command_name: str,
            parameters_to_secure: dict,
            secure_parameters=None,
            is_flat_request: bool = False,
    ):
        if not secure_parameters:
            secure_parameters = []
        secured_parameters = {
            k: (v if k not in secure_parameters else '*****')
            for k, v in parameters_to_secure.items()
        }
        return self._build_message(
            id=id,
            command_name=command_name,
            parameters=secured_parameters,
            is_flat_request=is_flat_request,
        )
