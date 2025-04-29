from modular_sdk.commons.constants import Env
from modular_sdk.commons.helpers import classproperty

TABLES_PREFIX = ''


class BaseMeta:
    """
    Allows using separate set of credentials to access Dynamodb
    """

    @classproperty
    def aws_access_key_id(cls):
        return Env.INNER_AWS_ACCESS_KEY_ID.get()

    @classproperty
    def aws_secret_access_key(cls):
        return Env.INNER_AWS_SECRET_ACCESS_KEY.get()

    @classproperty
    def aws_session_token(cls):
        return Env.INNER_AWS_SESSION_TOKEN.get()

    @classproperty
    def region(cls):
        return Env.ASSUME_ROLE_REGION.get() or Env.AWS_REGION.get()
