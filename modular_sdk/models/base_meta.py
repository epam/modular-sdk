import os

from modular_sdk.commons.constants import MODULAR_AWS_ACCESS_KEY_ID_ENV, \
    MODULAR_AWS_SECRET_ACCESS_KEY_ENV, MODULAR_AWS_SESSION_TOKEN_ENV, REGION_ENV, \
    MODULAR_REGION_ENV
from modular_sdk.commons.helpers import classproperty

TABLES_PREFIX = ''
ENV_VAR_REGION = REGION_ENV
ENV_VAR_MODULAR_REGION = MODULAR_REGION_ENV


class BaseMeta:
    """
    Allows using separate set of credentials to access Dynamodb
    """
    @classproperty
    def aws_access_key_id(cls):
        return os.environ.get(MODULAR_AWS_ACCESS_KEY_ID_ENV)

    @classproperty
    def aws_secret_access_key(cls):
        return os.environ.get(MODULAR_AWS_SECRET_ACCESS_KEY_ENV)

    @classproperty
    def aws_session_token(cls):
        return os.environ.get(MODULAR_AWS_SESSION_TOKEN_ENV)

    @classproperty
    def region(cls):
        return os.environ.get(ENV_VAR_MODULAR_REGION) or \
               os.environ.get(ENV_VAR_REGION)
