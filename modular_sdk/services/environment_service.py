import os
from typing import List, Optional

from modular_sdk.commons.constants import (
    ENVS_TO_HIDE,
    HIDDEN_ENV_PLACEHOLDER,
    Env,
    ServiceMode,
)
from modular_sdk.commons.log_helper import get_logger

_LOG = get_logger(__name__)


class EnvironmentService:
    def __init__(self):
        self._environment = os.environ

    def __repr__(self) -> str:
        return ', '.join(
            [
                f'{k}={v if k not in ENVS_TO_HIDE else HIDDEN_ENV_PLACEHOLDER}'
                for k, v in self._environment.items()
            ]
        )

    def set(self, name: str, value: str):
        self._environment[name] = value

    def aws_region(self) -> str:
        return Env.AWS_REGION.get()

    def default_aws_region(self) -> str:
        return Env.AWS_DEFAULT_REGION.get()

    def is_docker(self) -> bool:
        return Env.SERVICE_MODE.get() == ServiceMode.DOCKER

    def component(self):
        return Env.COMPONENT_NAME.get()

    def application(self):
        return Env.APPLICATION_NAME.get()

    def queue_url(self) -> Optional[str]:
        return Env.QUEUE_URL.get()

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
        env = Env.ASSUME_ROLE_ARN.get()
        if not env:  # None or ''
            return []
        return env.split(',')

    def modular_aws_credentials_expiration(self) -> Optional[str]:
        """
        UTC iso
        """
        return Env.INNER_AWS_CREDENTIALS_EXPIRATION.get()

    def modular_aws_access_key_id(self) -> Optional[str]:
        return Env.INNER_AWS_ACCESS_KEY_ID.get()

    def modular_aws_secret_access_key(self) -> Optional[str]:
        return Env.INNER_AWS_SECRET_ACCESS_KEY.get()

    def modular_aws_session_token(self) -> Optional[str]:
        return Env.INNER_AWS_SESSION_TOKEN.get()

    def modular_aws_region(self) -> Optional[str]:
        return Env.ASSUME_ROLE_REGION.get()

    def inner_cache_ttl_seconds(self) -> int:
        """
        Used for cachetools.TTLCache.
        Currently used in the caching wrapper of ssm service
        :return:
        """
        from_env = Env.INNER_CACHE_TTL_SECONDS.get()
        if from_env.isdigit():
            return int(from_env)
        return int(Env.INNER_CACHE_TTL_SECONDS.default)


class EnvironmentContext:
    """
    Use it with credentials
    """

    def __init__(
        self, envs: Optional[dict] = None, reset_all: Optional[bool] = True
    ):
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
        return {k: str(v) for k, v in envs.items() if v}

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
