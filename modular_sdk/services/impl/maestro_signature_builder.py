import base64
import binascii
import hashlib
import hmac
import json
import os
import time
from modular_sdk.commons.constants import PLAIN_CONTENT_TYPE


class MaestroSignatureBuilder:
    __slots__ = '_access_key', '_secret_key', '_user'

    def __init__(self, access_key: str, secret_key: str, user: str):
        self._access_key = access_key
        self._secret_key = secret_key
        self._user = user

    def decrypt(self, data: bytes | str) -> bytes:
        """
        Decode received message from Base64 format, cut initialization
        vector ("iv") from beginning of the message, decrypt message
        """
        from cryptography.hazmat.primitives.ciphers.aead import AESGCM

        try:
            decoded_data = base64.b64decode(data, validate=True)
        except binascii.Error as e:
            raise ValueError('Encrypted payload must be valid base64') from e

        # AES-GCM payload = 12-byte IV + ciphertext + 16-byte auth tag.
        if len(decoded_data) < 28:
            raise ValueError('Encrypted payload is too short')

        iv = decoded_data[:12]
        encrypted_data = decoded_data[12:]  # ciphertext + 16-byte GCM auth tag
        try:
            cipher = AESGCM(key=self._secret_key.encode('utf-8'))
        except ValueError as e:
            raise ValueError(str(e).replace('AESGCM key', 'Secret Key'))
        return cipher.decrypt(
            nonce=iv,
            data=encrypted_data,
            associated_data=None,
        )

    def encrypt(self, data: str | dict | list) -> bytes:
        """
        Encrypt data, add initialization vector ("iv") at beginning of encrypted
        message and encode entire data in Base64 format
        """
        from cryptography.hazmat.primitives.ciphers.aead import AESGCM
        iv = os.urandom(12)
        plain_text = data if isinstance(data, str) else json.dumps(
            data,
            separators=(',', ':')
        )
        data_in_bytes = plain_text.encode('utf-8')
        try:
            cipher = AESGCM(key=self._secret_key.encode('utf-8'))
        except ValueError as e:
            raise ValueError(str(e).replace('AESGCM key', 'Secret Key'))
        encrypted_data = cipher.encrypt(
            nonce=iv, data=data_in_bytes, associated_data=None)
        encrypted_data_with_iv = bytes(iv) + encrypted_data
        return base64.b64encode(encrypted_data_with_iv)

    def get_signed_headers(self, async_request: bool = False,
                           compressed: bool = False) -> dict:
        """
        Create and sign necessary headers for interaction with Maestro API
        """
        date = int(time.time() * 1000)
        signature = hmac.new(
            key=bytearray(f'{self._secret_key}{date}'.encode('utf-8')),
            msg=bytearray(
                f'M3-POST:{self._access_key}:{date}:{self._user}'.encode(
                    'utf-8')
                ),
            digestmod=hashlib.sha256
        ).hexdigest()
        n = 2
        resolved_signature = ''
        for each in [signature[i:i + n] for i in range(0, len(signature), n)]:
            resolved_signature += '1' + each
        return {
            "maestro-authentication": resolved_signature,
            "maestro-request-identifier": "api-server",
            "maestro-user-identifier": self._user,
            "maestro-date": str(date),
            "maestro-accesskey": str(self._access_key),
            "maestro-sdk-version": "3.2.80",
            "maestro-sdk-async": 'true' if async_request else 'false',
            "compressed": True if compressed else False,
        }

    def get_http_signed_headers(self, async_request: bool = False, 
                                compressed: bool = False) -> dict:
        base = self.get_signed_headers(async_request=async_request, 
                                       compressed=compressed)
        base['compressed'] = 'true' if base['compressed'] else 'false'
        base['Content-Type'] = PLAIN_CONTENT_TYPE
        return base

