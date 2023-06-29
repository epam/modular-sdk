import os
from typing import Optional, List

from modular_sdk.commons.constants import PARAM_ASSUME_ROLE_ARN, \
    MODULAR_AWS_CREDENTIALS_EXPIRATION_ENV, REGION_ENV, ENVS_TO_HIDE, \
    HIDDEN_ENV_PLACEHOLDER, MODULAR_AWS_SESSION_TOKEN_ENV, \
    MODULAR_AWS_ACCESS_KEY_ID_ENV, MODULAR_AWS_SECRET_ACCESS_KEY_ENV, \
    MODULAR_REGION_ENV, MODULAR_SERVICE_MODE_ENV, SERVICE_MODE_DOCKER, \
    DEFAULT_REGION_ENV, ENV_INNER_CACHE_TTL_SECONDS, \
    DEFAULT_INNER_CACHE_TTL_SECONDS
from modular_sdk.commons.log_helper import get_logger

_LOG = get_logger(__name__)


class EnvironmentService:
    # TODO make it decent and put envs' names to constants
    def __init__(self):
        self._environment = os.environ

    def __repr__(self) -> str:
        return ', '.join([
            f'{k}={v if k not in ENVS_TO_HIDE else HIDDEN_ENV_PLACEHOLDER}'
            for k, v in self._environment.items()
        ])

    def set(self, name: str, value: str):
        self._environment[name] = value

    def aws_region(self) -> str:
        return self._environment.get(REGION_ENV)

    def default_aws_region(self) -> str:
        return self._environment.get(DEFAULT_REGION_ENV)

    def is_docker(self) -> bool:
        return self._environment.get(MODULAR_SERVICE_MODE_ENV) == SERVICE_MODE_DOCKER

    def component(self):
        return self._environment.get('component_name')

    def application(self):
        return self._environment.get('application_name')

    def queue_url(self) -> Optional[str]:
        return self._environment.get('queue_url')

    def modular_assume_role_arn(self) -> List[str]:
        """
        Returns a list of roles to assume before making requests to
        DynamoDB and SSM (currently only DynamoDB and SSM).
        They are assumed one by another in order to be able to organize
        convenient access to another AWS account. For example:
            we have Custodian prod and Modular prod (these are different
            AWS accounts). Custodian must query some tables from Modular prod.
            To make this work, we create:
            - a role on Modular prod which provides access to Modular tables;
            - a role on Custodian prod which can assume the role from Modular
            prod and can BE assumed by roles of each of our lambdas.
            What it gives us? - we don't have to change the trusted
            relationships of the role from Modular prod (perceive Modular
            prod as something far and external) in case some of our roles
            changed their names, or we add new lambdas/roles
        :return:
        """
        env = self._environment.get(PARAM_ASSUME_ROLE_ARN)
        if not env:  # None or ''
            return []
        return env.split(',')

    def modular_aws_credentials_expiration(self) -> Optional[str]:
        """
        UTC iso
        """
        return self._environment.get(MODULAR_AWS_CREDENTIALS_EXPIRATION_ENV)

    def modular_aws_access_key_id(self) -> Optional[str]:
        return self._environment.get(MODULAR_AWS_ACCESS_KEY_ID_ENV)

    def modular_aws_secret_access_key(self) -> Optional[str]:
        return self._environment.get(MODULAR_AWS_SECRET_ACCESS_KEY_ENV)

    def modular_aws_session_token(self) -> Optional[str]:
        return self._environment.get(MODULAR_AWS_SESSION_TOKEN_ENV)

    def modular_aws_region(self) -> Optional[str]:
        return self._environment.get(MODULAR_REGION_ENV)

    def inner_cache_ttl_seconds(self) -> int:
        """
        Used for cachetools.TTLCache.
        Currently used in the caching wrapper of ssm service
        :return:
        """
        from_env = str(self._environment.get(ENV_INNER_CACHE_TTL_SECONDS))
        if from_env.isdigit():
            return int(from_env)
        return DEFAULT_INNER_CACHE_TTL_SECONDS


class EnvironmentContext:
    """
    Use it with credentials
    """

    def __init__(self, envs: Optional[dict] = None,
                 reset_all: Optional[bool] = True):
        self.envs = envs
        self._reset_all = reset_all
        self._is_set = False
        self._old_envs: dict = {}

    @property
    def envs(self) -> dict:
        return self._envs

    @envs.setter
    def envs(self, value: Optional[dict]):
        self._envs = self._adjust_envs(value or {})

    @staticmethod
    def _adjust_envs(envs: dict) -> dict:
        return {
            k: str(v) for k, v in envs.items() if v
        }

    def set(self):
        self._old_envs.update(os.environ)
        _LOG.info(f'Setting {", ".join(self._envs.keys())} envs')
        os.environ.update(self._envs)
        self._is_set = True

    def clear(self):
        _LOG.info(f'Unsetting {", ".join(self._envs.keys())} envs')
        if self._reset_all:
            os.environ.clear()
            os.environ.update(self._old_envs)
        else:
            [os.environ.pop(key, None) for key in self.envs]
        self._old_envs.clear()
        self._is_set = False

    def __enter__(self):
        self.set()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.clear()
