import json
import os
import re
from abc import ABC, abstractmethod
from typing import Union, Dict, Optional, List

from botocore.credentials import JSONFileCache
from botocore.exceptions import ClientError
from cachetools import TTLCache

from modular_sdk.commons.log_helper import get_logger
from modular_sdk.commons.time_helper import utc_datetime
from modular_sdk.services.aws_creds_provider import AWSCredentialsProvider, \
    ModularAssumeRoleClient
from modular_sdk.services.environment_service import EnvironmentService

_LOG = get_logger(__name__)

SSM_NOT_AVAILABLE = re.compile(r'[^a-zA-Z0-9\/_.-]')
SecretValue = Union[Dict, List, str]


class AbstractSSMClient(ABC):

    @staticmethod
    def allowed_name(name: str) -> str:
        """
        Keeps only allowed symbols
        """
        return str(re.sub(SSM_NOT_AVAILABLE, '-', name))

    def safe_name(self, name: str, prefix: Optional[str] = None,
                  date: Optional[bool] = True) -> str:
        if prefix:
            name = f'{prefix}.{name}'
        if date:
            name = f'{name}.{utc_datetime().strftime("%m.%d.%Y.%H.%M.%S")}'
        return self.allowed_name(name)

    @abstractmethod
    def get_parameter(self, name: str) -> Optional[SecretValue]:
        ...

    @abstractmethod
    def put_parameter(self, name: str, value: SecretValue,
                      _type='SecureString') -> Optional[str]:
        ...

    @abstractmethod
    def delete_parameter(self, name: str) -> bool:
        ...


class OnPremSSMClient(AbstractSSMClient):
    """
    The purpose is only debug and local testing. It must not be used as
    prod environment because it's not secure at all. Here I just
    emulate some parameter store. In case we really need on-prem,
    we must use Vault
    """
    path = os.path.expanduser(os.path.join('~', '.modular_sdk', 'on-prem', 'ssm'))

    def __init__(self):
        self._store = JSONFileCache(self.path)

    def put_parameter(self, name: str, value: SecretValue,
                      _type='SecureString') -> Optional[str]:
        self._store[name] = value
        return name

    def get_parameter(self, name: str) -> Optional[SecretValue]:
        if name in self._store:
            return self._store[name]

    def delete_parameter(self, name: str) -> bool:
        if name in self._store:
            del self._store[name]
            return True
        return False


class VaultSSMClient(AbstractSSMClient):
    mount_point = 'kv'
    key = 'data'

    def __init__(self):
        self._client = None  # hvac.Client

    def _init_client(self):
        import hvac
        # TODO use some discussed constants. These I get from Custodian
        vault_token = os.getenv('VAULT_TOKEN')
        vault_host = os.getenv('VAULT_URL')
        vault_port = os.getenv('VAULT_SERVICE_SERVICE_PORT')
        _LOG.info('Initializing hvac client')
        self._client = hvac.Client(
            url=f'http://{vault_host}:{vault_port}',
            token=vault_token
        )
        _LOG.info('Hvac client was initialized')

    @property
    def client(self):
        if not self._client:
            self._init_client()
        return self._client

    def get_parameter(self, name: str) -> Optional[SecretValue]:
        try:
            response = self.client.secrets.kv.v2.read_secret_version(
                path=name, mount_point=self.mount_point) or {}
        except Exception:  # hvac.InvalidPath
            return
        return response.get('data', {}).get('data', {}).get(self.key)

    def put_parameter(self, name: str, value: SecretValue,
                      _type='SecureString') -> Optional[str]:
        self.client.secrets.kv.v2.create_or_update_secret(
            path=name,
            secret={self.key: value},
            mount_point=self.mount_point
        )
        return name

    def delete_parameter(self, name: str) -> bool:
        return bool(self.client.secrets.kv.v2.delete_metadata_and_all_versions(
            path=name, mount_point=self.mount_point))


class SSMService(AWSCredentialsProvider,  # actually it's a client
                 AbstractSSMClient):
    def __init__(self, **kwargs):
        kwargs['service_name'] = 'ssm'
        super().__init__(**kwargs)

    def get_parameter(self, name: str) -> Optional[SecretValue]:
        try:
            response = self.client.get_parameter(
                Name=name,
                WithDecryption=True
            )
            value_str = response['Parameter']['Value']
            try:
                return json.loads(value_str)
            except json.JSONDecodeError:
                return value_str
        except ClientError as e:
            error_code = e.response['Error']['Code']
            _LOG.error(f'Can\'t get secret for name \'{name}\', '
                       f'error code: \'{error_code}\'')
            return

    def put_parameter(self, name: str, value: SecretValue,
                      _type='SecureString') -> Optional[str]:
        """
        In case the secret was saved successfully, its real name is returned.
        (the name can differ from the given one).
        In case something went wrong, None is returned
        """
        try:
            if isinstance(value, (list, dict)):
                value = json.dumps(value, separators=(",", ":"),
                                   sort_keys=True)
            self.client.put_parameter(
                Name=name,
                Value=value,
                Overwrite=True,
                Type=_type)
            return name
        except ClientError as e:
            error_code = e.response['Error']['Code']
            _LOG.error(f'Can\'t put secret for name \'{name}\', '
                       f'error code: \'{error_code}\'')
            return

    def delete_parameter(self, name: str) -> bool:
        try:
            self.client.delete_parameter(Name=name)
        except ClientError as e:
            error_code = e.response['Error']['Code']
            _LOG.error(f'Can\'t delete secret name \'{name}\', '
                       f'error code: \'{error_code}\'')
            return False
        return True


class ModularAssumeRoleSSMService(SSMService):
    client = ModularAssumeRoleClient('ssm')

    def __init__(self, *args, **kwargs):
        """
        No need
        """


class SSMClientCachingWrapper(AbstractSSMClient):
    def __init__(self, client: AbstractSSMClient,
                 environment_service: EnvironmentService):
        self._client = client
        self._cache = TTLCache(
            maxsize=50, ttl=environment_service.inner_cache_ttl_seconds()
        )

    @property
    def client(self) -> AbstractSSMClient:
        return self._client

    def get_parameter(self, name: str) -> Optional[SecretValue]:
        if name in self._cache:
            return self._cache[name]
        value = self.client.get_parameter(name)
        if value:
            self._cache[name] = value
        return value

    def put_parameter(self, name: str, value: SecretValue,
                      _type='SecureString') -> Optional[str]:
        name = self.client.put_parameter(name, value, _type)
        if name:
            self._cache[name] = value
        return name

    def delete_parameter(self, name: str) -> bool:
        self._cache.pop(name, None)
        return self.client.delete_parameter(name)
