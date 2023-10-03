import base64
import binascii
import gzip
import hashlib
import hmac
import json
import os
import uuid
from datetime import datetime

from modular_sdk.commons import ModularException
from modular_sdk.commons.log_helper import get_logger
from modular_sdk.services.rabbit_transport_service import RabbitMQTransport, \
    RabbitConfig

_LOG = get_logger('rabbit_transport_service')

PLAIN_CONTENT_TYPE = 'text/plain'
SUCCESS_STATUS = 'SUCCESS'
ERROR_STATUS = 'FAILED'
RESULTS = 'results'
DATA = 'data'


class MaestroRabbitConfig(RabbitConfig):
    def __init__(self, request_queue, response_queue, rabbit_exchange,
                 sdk_access_key, sdk_secret_key, maestro_user):
        super(MaestroRabbitConfig, self).__init__(
            request_queue=request_queue,
            response_queue=response_queue,
            rabbit_exchange=rabbit_exchange
        )
        self.sdk_access_key = sdk_access_key
        self.sdk_secret_key = sdk_secret_key
        self.maestro_user = maestro_user


class MaestroRabbitMQTransport(RabbitMQTransport):
    def __init__(self, rabbit_connection, config):
        super().__init__(
            rabbit_connection=rabbit_connection,
            config=config
        )
        self.access_key = config.sdk_access_key
        self.secret_key = config.sdk_secret_key
        self.user = config.maestro_user

    def pre_process_request(self, command_name, parameters, secure_parameters,
                            is_flat_request, async_request, compressed=False,
                            config=None):
        request_id = self._generate_id()

        _LOG.debug('Going to pre-process request')
        message = self._build_message(
            command_name=command_name,
            parameters=parameters,
            id=request_id,
            is_flat_request=is_flat_request,
            compressed=compressed
        )
        secure_message = message
        if not compressed:
            secure_message = self._build_secure_message(
                command_name=command_name,
                parameters_to_secure=parameters,
                secure_parameters=secure_parameters,
                id=request_id,
                is_flat_request=is_flat_request
            )
        _LOG.debug('Prepared command: {0}\nCommand format: {1}'
                   .format(command_name, secure_message))

        encrypted_body = self._encrypt(
            secret_key=config.sdk_secret_key if config and config.sdk_secret_key else self.secret_key,
            data=message
        )
        _LOG.debug('Message encrypted')
        # sign headers
        headers = self._get_signed_headers(
            access_key=config.sdk_access_key if config and config.sdk_access_key else self.access_key,
            secret_key=config.sdk_secret_key if config and config.sdk_secret_key else self.secret_key,
            user=config.maestro_user if config and config.maestro_user else self.user,
            async_request=async_request,
            compressed=compressed
        )
        _LOG.debug('Signed headers prepared')
        return encrypted_body, headers

    def post_process_request(self, response):
        try:
            response_item = self._decrypt(
                secret_key=self.secret_key,
                data=response
            )
            _LOG.debug('Message from M3-server successfully decrypted')
        except binascii.Error:
            response_item = response.decode('utf-8')
        try:
            _LOG.debug(f'Raw decrypted message from server: {response_item}')
            response_json = json.loads(response_item).get('results')[0]
            status = response_json.get('status')
            code = response_json.get('statusCode')
            if status == SUCCESS_STATUS:
                data = response_json.get('data')
                return code, status, data
            else:
                data = response_json.get('readableError')
                return code, status, data

        except json.decoder.JSONDecodeError:
            _LOG.error('Response can not be decoded - invalid Json string')
            raise ModularException(
                code=502,
                content='Response can not be decoded'
            )

    @staticmethod
    def _generate_id():
        return str(uuid.uuid1())

    @staticmethod
    def _decrypt(secret_key, data):
        """
        Decode received message from Base64 format, cut initialization
        vector ("iv") from beginning of the message, decrypt message
        """
        from cryptography.hazmat.primitives.ciphers import Cipher, \
            algorithms, modes
        decoded_data = base64.b64decode(data)
        iv = decoded_data[:12]
        encrypted_data = decoded_data[12:]
        cipher = Cipher(
            algorithms.AES(key=secret_key.encode('utf-8')),
            modes.GCM(initialization_vector=iv)
        ).decryptor()
        origin_data_with_iv = cipher.update(encrypted_data)
        # Due to Initialization vector in encrypting method
        # there is need to split useful and useless parts of the
        # server response.
        response = origin_data_with_iv[:-16]
        return response

    @staticmethod
    def _encrypt(secret_key, data):
        """
        Encrypt data, add initialization vector ("iv") at beginning of encrypted
        message and encode entire data in Base64 format
        """
        if not secret_key:
            raise ModularException(
                code=503,
                content='Cannot detect secret_key. Please add it first'
            )
        from cryptography.hazmat.primitives.ciphers.aead import AESGCM
        iv = os.urandom(12)
        plain_text = data if isinstance(data, str) else json.dumps(data)
        data_in_bytes = plain_text.encode('utf-8')
        try:
            cipher = AESGCM(key=secret_key.encode('utf-8'))
        except ValueError as e:
            raise ValueError(str(e).replace('AESGCM key', 'Secret Key'))
        encrypted_data = cipher.encrypt(
            nonce=iv, data=data_in_bytes, associated_data=None)
        encrypted_data_with_iv = bytes(iv) + encrypted_data
        base64_request = base64.b64encode(encrypted_data_with_iv)
        return base64_request

    @staticmethod
    def _get_signed_headers(access_key: (str, int), secret_key: str, user: str,
                            async_request: bool = False,
                            compressed: bool = False) -> dict:
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
            msg=bytearray(f'M3-POST:{access_key}:{date}:{user}'.encode('utf-8')
                          ),
            digestmod=hashlib.sha256
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
            "compressed": True if compressed else False
        }
        return headers

    @staticmethod
    def _build_payload(id, command_name, parameters, is_flat_request):
        if is_flat_request:
            parameters.update({'type': command_name})
            result = [
                {
                    'id': id,
                    'type': None,
                    'params': parameters
                }
            ]
        else:
            result = [
                {
                    'id': id,
                    'type': command_name,
                    'params': parameters
                }
            ]
        return result

    def _build_message(self, id, command_name, parameters,
                       is_flat_request=False, compressed=False):
        if isinstance(parameters, list):
            result = []
            for payload in parameters:
                result.extend(self._build_payload(id, command_name, payload,
                                                  is_flat_request))
        else:
            result = self._build_payload(id, command_name, parameters,
                                         is_flat_request)
        if compressed:
            return base64.b64encode(gzip.compress(
                json.dumps(result).encode('UTF-8'))).decode()
        return result

    def _build_secure_message(self, id, command_name, parameters_to_secure,
                              secure_parameters=None, is_flat_request=False):
        if not secure_parameters:
            secure_parameters = []
        secured_parameters = {k: (v if k not in secure_parameters else '*****')
                              for k, v in parameters_to_secure.items()}
        return self._build_message(
            command_name=command_name,
            parameters=secured_parameters,
            id=id,
            is_flat_request=is_flat_request
        )
