from unittest.mock import patch

from modular_sdk.commons.constants import Env


def test_env_aliases_resolving():
    with patch('os.environ', {'modular_mongo_uri': 'mongo://test'}):
        assert Env.MONGO_URI.get() == 'mongo://test'

    with patch('os.environ', {'MODULAR_SDK_MONGO_URI': 'mongo://test'}):
        assert Env.MONGO_URI.get() == 'mongo://test'

    with patch('os.environ', {'modular_mongo_uri': 'mongo://test2',
                              'MODULAR_SDK_MONGO_URI': 'mongo://test1'}):
        assert Env.MONGO_URI.get() == 'mongo://test1'


def test_env_default():
    with patch('os.environ', {}):
        assert Env.MONGO_URI.get() is None
        assert Env.MONGO_URI.get('mongo://test') == 'mongo://test'
        assert Env.LOG_LEVEL.get() == 'INFO'
        assert Env.LOG_LEVEL.get('DEBUG') == 'DEBUG'
    with patch('os.environ', {'MODULAR_SDK_LOG_LEVEL': 'WARNING'}):
        assert Env.LOG_LEVEL.get() == 'WARNING'
        assert Env.LOG_LEVEL.get('DEBUG') == 'WARNING'


def test_env_default_callable():
    with patch('os.environ', {'MODULAR_SDK_SERVICE_MODE': 'docker'}):
        assert Env.SERVICE_MODE.get() == 'docker'
        assert Env.DB_BACKEND.get() == 'mongo'
        assert Env.SECRETS_BACKEND.get() == 'vault'
    with patch('os.environ', {}):
        assert Env.SERVICE_MODE.get() == 'saas'
        assert Env.DB_BACKEND.get() == 'mongo'
        assert Env.SECRETS_BACKEND.get() == 'ssm'

    with patch('os.environ', {'MODULAR_SDK_DB_BACKEND': 'mongo', 'MODULAR_SDK_SECRETS_BACKEND': 'ssm'}):
        assert Env.SERVICE_MODE.get() == 'saas'
        assert Env.DB_BACKEND.get() == 'mongo'
        assert Env.SECRETS_BACKEND.get() == 'ssm'
