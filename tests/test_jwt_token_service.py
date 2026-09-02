import pytest

from modular_sdk.services.jwt_token_service import JWTTokenService


class TestJWTTokenService:
    SECRET_KEY = 'test-secret-key-for-testing-only'
    ALGORITHM = 'HS256'

    @pytest.fixture
    def jwt_service(self):
        """Fixture to create JWT service instance."""
        return JWTTokenService(
            secret_key=self.SECRET_KEY,
            algorithm=self.ALGORITHM
        )

    def test_init_with_valid_key(self, jwt_service):
        """Test service initialization with valid secret key."""
        assert jwt_service.secret_key == self.SECRET_KEY
        assert jwt_service.algorithm == self.ALGORITHM

    def test_init_with_empty_key(self):
        """Test service initialization fails with empty secret key."""
        with pytest.raises(ValueError, match='secret_key cannot be empty'):
            JWTTokenService(secret_key='')

    def test_init_with_none_key(self):
        """Test service initialization fails with None secret key."""
        with pytest.raises(ValueError, match='secret_key cannot be empty'):
            JWTTokenService(secret_key=None)

    def test_encode_valid_payload(self, jwt_service):
        """Test encoding a valid payload."""
        payload = {'user_id': '123', 'username': 'testuser'}
        token = jwt_service.encode(payload)

        assert isinstance(token, str)
        assert len(token) > 0
        assert token.count('.') == 2

    def test_encode_with_expiration(self, jwt_service):
        """Test encoding with expiration time."""
        payload = {'user_id': '123'}
        expires_in = 3600
        token = jwt_service.encode(payload, expires_in=expires_in)

        assert isinstance(token, str)
        decoded = jwt_service.decode(token)
        assert 'exp' in decoded
        assert 'iat' in decoded

    def test_encode_empty_payload(self, jwt_service):
        """Test encoding fails with empty payload."""
        with pytest.raises(ValueError, match='payload cannot be empty'):
            jwt_service.encode({})

    def test_encode_none_payload(self, jwt_service):
        """Test encoding fails with None payload."""
        with pytest.raises(ValueError, match='payload cannot be empty'):
            jwt_service.encode(None)

    def test_decode_valid_token(self, jwt_service):
        """Test decoding a valid token."""
        original_payload = {'user_id': '123', 'username': 'testuser'}
        token = jwt_service.encode(original_payload)

        decoded_payload = jwt_service.decode(token)

        assert decoded_payload['user_id'] == original_payload['user_id']
        assert decoded_payload['username'] == original_payload['username']

    def test_decode_empty_token(self, jwt_service):
        """Test decoding fails with empty token."""
        with pytest.raises(ValueError, match='token cannot be empty'):
            jwt_service.decode('')

    def test_decode_none_token(self, jwt_service):
        """Test decoding fails with None token."""
        with pytest.raises(ValueError, match='token cannot be empty'):
            jwt_service.decode(None)

    def test_decode_invalid_token(self, jwt_service):
        """Test decoding fails with invalid token."""
        with pytest.raises(Exception):
            jwt_service.decode('invalid.token.here')

    def test_decode_expired_token(self, jwt_service):
        """Test decoding fails with expired token."""
        payload = {'user_id': '123'}
        token = jwt_service.encode(payload, expires_in=-1)

        with pytest.raises(Exception):
            jwt_service.decode(token)

    def test_decode_with_verification_disabled(self, jwt_service):
        """Test decoding expired token without verification."""
        payload = {'user_id': '123'}
        token = jwt_service.encode(payload, expires_in=-1)

        decoded = jwt_service.decode(token, verify_exp=False)
        assert decoded['user_id'] == payload['user_id']

    def test_is_token_valid_with_valid_token(self, jwt_service):
        """Test token validation with valid token."""
        payload = {'user_id': '123'}
        token = jwt_service.encode(payload)

        assert jwt_service.is_token_valid(token) is True

    def test_is_token_valid_with_invalid_token(self, jwt_service):
        """Test token validation with invalid token."""
        assert jwt_service.is_token_valid('invalid.token') is False

    def test_is_token_valid_with_expired_token(self, jwt_service):
        """Test token validation with expired token."""
        payload = {'user_id': '123'}
        token = jwt_service.encode(payload, expires_in=-1)

        assert jwt_service.is_token_valid(token) is False

    def test_is_token_valid_with_empty_token(self, jwt_service):
        """Test token validation with empty token."""
        assert jwt_service.is_token_valid('') is False

    def test_is_token_valid_with_none_token(self, jwt_service):
        """Test token validation with None token."""
        assert jwt_service.is_token_valid(None) is False

    def test_decode_without_verification(self, jwt_service):
        """Test decoding without verification."""
        payload = {'user_id': '123', 'username': 'testuser'}
        token = jwt_service.encode(payload)

        decoded = jwt_service.decode_without_verification(token)
        assert decoded['user_id'] == payload['user_id']
        assert decoded['username'] == payload['username']

    def test_decode_without_verification_expired_token(self, jwt_service):
        """Test decoding expired token without verification."""
        payload = {'user_id': '123'}
        token = jwt_service.encode(payload, expires_in=-1)

        decoded = jwt_service.decode_without_verification(token)
        assert decoded['user_id'] == payload['user_id']

    def test_decode_without_verification_empty_token(self, jwt_service):
        """Test decode without verification fails with empty token."""
        with pytest.raises(ValueError, match='token cannot be empty'):
            jwt_service.decode_without_verification('')

    def test_round_trip_encoding_decoding(self, jwt_service):
        """Test encoding and decoding round-trip."""
        original_payload = {
            'user_id': '456',
            'email': 'test@example.com',
            'role': 'admin',
            'data': {'key': 'value'}
        }

        token = jwt_service.encode(original_payload)
        decoded_payload = jwt_service.decode(token)

        for key, value in original_payload.items():
            assert decoded_payload[key] == value

    def test_encode_decode_with_different_algorithms(self):
        """Test encoding/decoding with HS512 algorithm."""
        secret = 'test-secret-for-hs512-testing-needs-to-be-long-enough'
        service = JWTTokenService(
            secret_key=secret,
            algorithm='HS512'
        )

        payload = {'user_id': '789', 'data': 'test'}
        token = service.encode(payload)

        decoded = service.decode(
            token,
            algorithms=['HS512']
        )
        assert decoded['user_id'] == payload['user_id']
        assert decoded['data'] == payload['data']

    def test_custom_algorithm_parameter(self):
        """Test service with custom algorithm."""
        service = JWTTokenService(
            secret_key='secret',
            algorithm='HS512'
        )
        assert service.algorithm == 'HS512'

        payload = {'test': 'data'}
        token = service.encode(payload)
        decoded = service.decode(token)
        assert decoded['test'] == payload['test']
