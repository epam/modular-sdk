from datetime import datetime, timedelta, timezone
from typing import Any, Dict

from modular_sdk.commons.log_helper import get_logger

_LOG = get_logger(__name__)


class JWTTokenService:
    """
    Service for encoding and decoding JWT tokens.
    Requires PyJWT to be installed: pip install PyJWT

    For HMAC algorithms (HS256, HS384, HS512):
        - Use same key for both encoding and decoding

    For RSA algorithms (RS256, RS384, RS512):
        - Encoding: use private key
        - Decoding: use public key
    """

    def __init__(
        self,
        secret_key: str,
        algorithm: str | None = None,
        public_key: str | None = None,
    ):
        """
        Initialize JWT Token Service.

        :param secret_key: Secret key for signing tokens (or private key for RS*)
        :param algorithm: Algorithm to use (default: HS256)
        :param public_key: Public key for verifying tokens (only for RS* algorithms)
        """
        try:
            import jwt
            self.jwt = jwt
        except ImportError:
            raise ImportError(
                'PyJWT is required for JWTTokenService'
            )

        if not secret_key:
            raise ValueError('secret_key cannot be empty')

        self.secret_key = secret_key
        self.algorithm = algorithm or 'HS256'
        self.public_key = public_key

    def encode(
        self,
        payload: Dict[str, Any],
        expires_in: int | None = None,
    ) -> str:
        """
        Encode a JWT token.

        :param payload: Dictionary containing token claims
        :param expires_in: Token expiration time in seconds
                          (if None, token won't have expiration)
        :return: Encoded JWT token string
        :raises ValueError: If payload is empty or invalid
        """
        if not payload:
            raise ValueError('payload cannot be empty')

        token_payload = payload.copy()

        if expires_in:
            token_payload['exp'] = datetime.now(
                tz=timezone.utc
            ) + timedelta(seconds=expires_in)
            token_payload['iat'] = datetime.now(tz=timezone.utc)

        try:
            token = self.jwt.encode(
                token_payload, self.secret_key, algorithm=self.algorithm
            )
            _LOG.debug(f'Token encoded successfully with payload keys: '
                       f'{list(payload.keys())}')
            return token
        except Exception as e:
            _LOG.error(f'Error encoding JWT token: {str(e)}')
            raise

    def decode(
        self,
        token: str,
        verify_exp: bool = True,
        algorithms: list | None = None,
    ) -> Dict[str, Any]:
        """
        Decode and verify a JWT token.

        :param token: JWT token string to decode
        :param verify_exp: Whether to verify token expiration
                          (default: True)
        :param algorithms: List of allowed algorithms
                          (default: uses service's algorithm)
        :return: Decoded token payload as dictionary
        :raises ValueError: If token is empty
        :raises jwt.DecodeError: If token is invalid
        :raises jwt.ExpiredSignatureError: If token is expired
        """
        if not token:
            raise ValueError('token cannot be empty')

        if algorithms is None:
            algorithms = [self.algorithm]

        # For RS* algorithms, use public_key; for HS* use secret_key
        verify_key = self.public_key or self.secret_key

        try:
            decoded = self.jwt.decode(
                token,
                verify_key,
                algorithms=algorithms,
                options={'verify_exp': verify_exp},
            )
            _LOG.debug(f'Token decoded successfully with payload keys: '
                       f'{list(decoded.keys())}')
            return decoded
        except self.jwt.ExpiredSignatureError as e:
            _LOG.warning(f'Token has expired: {str(e)}')
            raise
        except self.jwt.DecodeError as e:
            _LOG.warning(f'Error decoding JWT token: {str(e)}')
            raise
        except Exception as e:
            _LOG.error(f'Unexpected error decoding JWT token: {str(e)}')
            raise

    def is_token_valid(self, token: str) -> bool:
        """
        Check if a token is valid (well-formed and not expired).

        :param token: JWT token string to validate
        :return: True if token is valid, False otherwise
        """
        if not token:
            return False

        try:
            self.decode(token, verify_exp=True)
            return True
        except Exception as e:
            _LOG.error(f'JWT token is invalid. Decoding error: {str(e)}')
            return False

    def decode_without_verification(self, token: str) -> Dict[str, Any]:
        """
        Decode token without verifying signature or expiration.
        Use with caution - only for inspection purposes.

        :param token: JWT token string to decode
        :return: Decoded token payload
        :raises ValueError: If token is empty
        :raises jwt.DecodeError: If token format is invalid
        """
        if not token:
            raise ValueError('token cannot be empty')

        try:
            decoded = self.jwt.decode(
                token,
                options={'verify_signature': False},
            )
            _LOG.debug('Token decoded without verification')
            return decoded
        except Exception as e:
            _LOG.error(f'Error decoding token without verification: '
                       f'{str(e)}')
            raise
