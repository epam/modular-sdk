import base64
import dataclasses
import io
import json
import tempfile
import uuid
from contextlib import contextmanager
from functools import cached_property
from pathlib import Path
from time import time
from typing import Optional, Union, Dict, Callable, TypeVar, TypedDict, Literal

from botocore.exceptions import ClientError
from urllib3.util import parse_url, Url

from modular_sdk.commons import DataclassBase
from modular_sdk.commons.constants import DEFAULT_AWS_REGION, \
    ENV_AZURE_SUBSCRIPTION_ID, ENV_CLOUDSDK_CORE_PROJECT, HTTPS_ATTR, \
    HTTP_ATTR, Cloud, ApplicationType
from modular_sdk.commons.log_helper import get_logger
from modular_sdk.models.application import Application
from modular_sdk.models.parent import Parent
from modular_sdk.models.tenant import Tenant
from modular_sdk.modular import Modular
from modular_sdk.services.application_service import ApplicationService
from modular_sdk.services.environment_service import EnvironmentService, \
    EnvironmentContext
from modular_sdk.services.parent_service import ParentService
from modular_sdk.services.ssm_service import AbstractSSMClient
from modular_sdk.services.sts_service import StsService
from modular_sdk.services.tenant_service import TenantService

# GOOGLE
MA_SSM_PROJECT_ID = 'project_id'

_LOG = get_logger(__name__)


@dataclasses.dataclass()
class AccessMeta(DataclassBase):
    """
    Common model to keep access data. It works this way:
    >>> meta = AccessMeta.from_dict({})
    >>> meta.update_host(host='https://epam.com/hello')
    >>> meta.dict()
    {'host': 'epam.com', 'stage': 'hello', 'port': 443, 'protocol': 'HTTPS'}
    >>> meta.update_host(host='https://epam.com/hello', port=80, protocol='http', stage='/dev')
    >>> meta.dict()
    {'host': 'epam.com', 'stage': 'dev', 'port': 80, 'protocol': 'HTTP'}
    """
    host: Optional[str]
    stage: Optional[str]  # path prefix without "/" in case it exists
    port: Optional[int]
    protocol: Optional[Literal['HTTP', 'HTTPS']]

    def update_host(self, host: Optional[str] = None,
                    port: Optional[int] = None,
                    protocol: Optional[str] = None,
                    stage: Optional[str] = None):
        """
        Use ONLY this method to update attributes
        :param host:
        :param port:
        :param protocol:
        :param stage:
        :return:
        """
        parsed: Url = parse_url(host)  # works with None
        _host = parsed.host
        _port = port or parsed.port or 443  # 443 default
        _stage = stage or parsed.path
        _protocol = protocol or parsed.scheme or \
                    (HTTPS_ATTR if _port == 443 else HTTP_ATTR)

        if _host:
            self.host = _host
        if _port:
            self.port = _port
        if _stage:
            self.stage = _stage.strip('/')
        if _protocol:
            self.protocol = _protocol.upper()

    @property
    def url(self) -> Optional[str]:
        if not self.host:
            return
        url = self.host.strip('/')  # assuming that host is without protocol
        if self.port:
            url += f':{self.port}'
        if self.protocol:
            url = self.protocol.lower() + '://' + url
        if self.stage:
            url += '/' + self.stage
        return url


# Applications meta and secrets definitions

@dataclasses.dataclass()
class DefectDojoApplicationMeta(AccessMeta):
    """
    Application with type 'DEFECT_DOJO'
    """


@dataclasses.dataclass(repr=False)
class DefectDojoApplicationSecret(DataclassBase):
    """
    Application with type 'CUSTODIAN' secret
    """
    api_key: str


@dataclasses.dataclass()
class CustodianApplicationMeta(AccessMeta):
    """
    Application with type 'CUSTODIAN' meta
    """
    username: Optional[str]
    results_storage: Optional[str]


@dataclasses.dataclass()
class RabbitMQApplicationMeta(DataclassBase):
    """
    Application with type 'RABBITMQ' meta
    """
    maestro_user: Optional[str] = None
    rabbit_exchange: Optional[str] = None
    request_queue: Optional[str] = None
    response_queue: Optional[str] = None
    sdk_access_key: Optional[str] = None


@dataclasses.dataclass(repr=False)
class RabbitMQApplicationSecret(DataclassBase):
    """
    Application with type 'RABBITMQ' secret
    """
    connection_url: str
    sdk_secret_key: str


@dataclasses.dataclass()
class AWSRoleApplicationMeta(DataclassBase):
    """
    Application with type 'AWS_ROLE' meta
    """
    roleName: str
    accountNumber: Optional[str] = None
    uuid: str = dataclasses.field(default_factory=lambda: str(uuid.uuid4()))


@dataclasses.dataclass()
class AWSCredentialsApplicationMeta(DataclassBase):
    """
    Application with type 'AWS_CREDENTIALS' meta
    """
    accountNumber: str
    uuid: str = dataclasses.field(default_factory=lambda: str(uuid.uuid4()))


@dataclasses.dataclass(repr=False)
class AWSCredentialsApplicationSecret(DataclassBase):
    """
    Application with type 'AWS_CREDENTIALS' secret
    """
    accessKeyId: str
    secretAccessKey: str
    sessionToken: Optional[str] = None
    defaultRegion: Optional[str] = None


@dataclasses.dataclass()
class AZURECredentialsApplicationMeta(DataclassBase):
    """
    Application with type 'AZURE_CREDENTIALS' meta
    """
    clientId: Optional[str] = None
    tenantId: Optional[str] = None
    uuid: str = dataclasses.field(default_factory=lambda: str(uuid.uuid4()))


@dataclasses.dataclass(repr=False)
class AZURECredentialsApplicationSecret(DataclassBase):
    """
    Application with type 'AZURE_CREDENTIALS' secret
    """
    client_id: str
    tenant_id: str
    api_key: str


@dataclasses.dataclass()
class AZURECertificateApplicationMeta(DataclassBase):
    """
    Application with type 'AZURE_CERTIFICATE' meta
    """
    clientId: Optional[str] = None
    tenantId: Optional[str] = None
    uuid: str = dataclasses.field(default_factory=lambda: str(uuid.uuid4()))


@dataclasses.dataclass(repr=False)
class AZURECertificateApplicationSecret(DataclassBase):
    """
    Application with type 'AZURE_CERTIFICATE' secret
    """
    certificate_base64: str
    certificate_password: Optional[str] = None


@dataclasses.dataclass()
class GCPServiceAccountApplicationMeta(DataclassBase):
    """
    Application with type 'GCP_SERVICE_ACCOUNT', 'GCP_COMPUTE_ACCOUNT' meta
    """
    adminProjectId: Optional[str] = None
    uuid: str = dataclasses.field(default_factory=lambda: str(uuid.uuid4()))


# credentials definitions
class GOOGLECredentialsRaw1(TypedDict):
    type: str
    project_id: str
    private_key_id: str
    private_key: str
    client_email: str
    client_id: str
    auth_uri: str
    token_uri: str
    auth_provider_x509_cert_url: str
    client_x509_cert_url: str


# ----- not used currently -----
# class GOOGLECredentialsRaw2(TypedDict):
#     type: str
#     access_token: str
#     refresh_token: str
#     client_id: str
#     client_secret: str
#     project_id: str
#
#
# class GOOGLECredentialsRaw3(TypedDict):
#     access_token: str
#     project_id: str
# ----- not used currently -----


# K8SKubeConfigApplicationSecret contains raw kubeconfig


class _CredentialsBase(DataclassBase):
    """
    Some useful method for credentials.
    """

    @property
    @contextmanager
    def export(self):
        _context = EnvironmentContext(self.dict(), reset_all=False)
        _context.set()
        try:
            yield
        finally:
            _context.clear()


@dataclasses.dataclass(frozen=True, repr=False)
class AWSCredentials(_CredentialsBase):
    AWS_ACCESS_KEY_ID: str
    AWS_SECRET_ACCESS_KEY: str
    AWS_SESSION_TOKEN: Optional[str] = None
    AWS_DEFAULT_REGION: Optional[str] = DEFAULT_AWS_REGION


@dataclasses.dataclass(frozen=True, repr=False)
class AZURECredentials(_CredentialsBase):
    AZURE_TENANT_ID: str
    AZURE_CLIENT_ID: str
    AZURE_CLIENT_SECRET: str
    AZURE_SUBSCRIPTION_ID: Optional[str] = None


@dataclasses.dataclass(frozen=True, repr=False)
class AZURECertificate(_CredentialsBase):
    AZURE_TENANT_ID: str
    AZURE_CLIENT_ID: str
    AZURE_CLIENT_CERTIFICATE_PATH: Path  # full path to file
    AZURE_CLIENT_CERTIFICATE_PASSWORD: Optional[str] = None
    AZURE_SUBSCRIPTION_ID: Optional[str] = None

    def get_raw(self) -> io.BytesIO:
        stream = io.BytesIO()
        with open(self.AZURE_CLIENT_CERTIFICATE_PATH, 'rb') as fp:
            stream.write(fp.read())
        stream.seek(0)
        return stream


@dataclasses.dataclass(frozen=True, repr=False)
class GOOGLECredentials(_CredentialsBase):
    GOOGLE_APPLICATION_CREDENTIALS: Path  # full path to file
    CLOUDSDK_CORE_PROJECT: Optional[str] = None

    def get_raw(self) -> GOOGLECredentialsRaw1:
        with open(self.GOOGLE_APPLICATION_CREDENTIALS, 'r') as fp:
            return json.load(fp)


@dataclasses.dataclass(frozen=True, repr=False)
class RabbitMQCredentials(_CredentialsBase):
    connection_url: str
    sdk_secret_key: str
    maestro_user: Optional[str] = None
    rabbit_exchange: Optional[str] = None
    request_queue: Optional[str] = None
    response_queue: Optional[str] = None
    sdk_access_key: Optional[str] = None


Credentials = Union[
    AWSCredentials,
    AZURECredentials,
    AZURECertificate,
    GOOGLECredentials,
    RabbitMQCredentials,
]


class MaestroCredentialsService:
    """
    Allows to retrieve credentials from Maestro applications. Each method
    returns a frozen dataclass which contain credentials in their
    environment variables format (meaning that if you convert the dataclass
    object to dict and export each key and value to envs, credentials
    should work). The basic flow is the following:

        mcs = Modular().maestro_credentials_service()
        # credentials = mcs.get_by_parent('<parent id>')  # or parent item
        # credentials = mcs.get_by_application('<application id>')  # or item
        credentials = mcs.get_by_tenant('DEV2')  # or item
        credentials = mcs.complete_credentials(credentials)
        if not credentials:
            return 'error'
        with credentials.export:
            boto3.client('sts').get_caller_identity()
        # or do something else with credentials

    Note: method `complete_credentials` is separated because in involves
    some logic which can be performed after credentials were retrieved
    from na application. For example, azure credentials require
    subscription id, but not all the applications contain it. So, we must
    get it from a tenant.

    """
    CT = TypeVar('CT', bound=Credentials)

    def __init__(self, tenant_service: TenantService,
                 parent_service: ParentService,
                 application_service: ApplicationService,
                 environment_service: EnvironmentService,
                 ssm_service: AbstractSSMClient,
                 sts_service: StsService):
        """
        In case the service is deployed to AWS, we apparently will need
        to go to another AWS account to get ssm secrets from Application.
        We use modular_client.assume_role_ssm_service for this.
        For on-prem currently we use our own service, because Modular
        does not support Vault.
        """
        self._tenant_service = tenant_service
        self._parent_service = parent_service
        self._application_service = application_service
        self._environment_service = environment_service
        self._ssm_service = ssm_service
        self._sts_service = sts_service

    @classmethod
    def build(cls, tenant_service: Optional[TenantService] = None,
              parent_service: Optional[ParentService] = None,
              application_service: Optional[ApplicationService] = None,
              environment_service: Optional[EnvironmentService] = None,
              ssm_service: Optional[AbstractSSMClient] = None,
              sts_service: Optional[StsService] = None
              ) -> 'MaestroCredentialsService':
        """
        Allows to build the service specifying some services to override.
        SSM Service is expected to be overriden because some applications
        (like AWS_ROLE, AZURE_CREDENTIALS) have their secrets on Maestro
        prod, and some applications (RABBITMQ) will probably have their
        secrets on our prod. So we need different ssm clients
        :return: MaestroCredentialsService.
        By default, maestro ssm client is used.
        """
        modular = Modular()
        return cls(
            tenant_service=tenant_service or modular.tenant_service(),
            parent_service=parent_service or modular.parent_service(),
            application_service=application_service or modular.application_service(),
            environment_service=environment_service or modular.environment_service(),
            ssm_service=ssm_service or modular.assume_role_ssm_service(),
            sts_service=sts_service or modular.sts_service()
        )

    def _assure_tenant_obj(self, tenant: Union[Tenant, str]
                           ) -> Optional[Tenant]:
        item = tenant if isinstance(tenant, Tenant) \
            else self._tenant_service.get(tenant)
        return item if item and item.is_active else None

    def _assure_application_obj(self, application: Union[Application, str]
                                ) -> Optional[Application]:
        item = application if isinstance(application, Application) \
            else self._application_service.get_application_by_id(application)
        return item if item and not item.is_deleted else None

    def _assure_parent_obj(self, parent: Union[Parent, str]
                           ) -> Optional[Parent]:
        item = parent if isinstance(parent, Parent) \
            else self._parent_service.get_parent_by_id(parent)
        return item if item and not item.is_deleted else None

    @staticmethod
    def _parent_id_from_tenant(tenant: Tenant) -> Optional[str]:
        return tenant.management_parent_id

    def _default_aws_region(self) -> str:
        return (self._environment_service.aws_region() or
                self._environment_service.default_aws_region() or
                DEFAULT_AWS_REGION)

    def complete_credentials(self, credentials: Optional[CT], tenant: Tenant,
                             **kwargs) -> Optional[CT]:
        """
        Some credentials (for example AZURE) must be expanded with
        subscription id which is sited withing a tenant but not application.
        Here we handle these special cases
        """
        if not credentials:
            return
        _LOG.info('Going to fulfill the credentials')
        return type(credentials)(**self.complete_credentials_dict(
            credentials=credentials.dict(),
            tenant=tenant,
            **kwargs
        ))

    @staticmethod
    def complete_credentials_dict(credentials: dict,
                                  tenant: Tenant, **kwargs) -> dict:
        _LOG.info('Going to fulfill the credentials')
        if tenant.cloud == Cloud.AZURE:
            _LOG.info('Tenant`s cloud is AZURE. Adding subscription id')
            if not credentials.get(ENV_AZURE_SUBSCRIPTION_ID):
                credentials[ENV_AZURE_SUBSCRIPTION_ID] = tenant.project
            return credentials
        elif tenant.cloud == Cloud.AWS:
            _LOG.info('Tenant`s cloud is AWS. Proxying creds')
            return credentials
        elif tenant.cloud == Cloud.GOOGLE:
            _LOG.info('Creds are requested for google tenant. '
                      'Adding project id')
            credentials[ENV_CLOUDSDK_CORE_PROJECT] = tenant.project
            _LOG.debug(f'Google credentials project_id: '
                       f'{credentials[ENV_CLOUDSDK_CORE_PROJECT]}')
            return credentials

        _LOG.info('Not known cloud. Proxying whatever was received')
        return credentials

    def get_by_tenant(self, tenant: Union[Tenant, str],
                      key: Optional[Callable[[Tenant], Optional[str]]] = None
                      ) -> Optional[Credentials]:
        """
        :param tenant: Union[Tenant, str]
        :param key: function which will retrieve parent_id from tenant.
        By default, management_parent_id is used
        :return:
        """
        tenant_obj = self._assure_tenant_obj(tenant)
        if not tenant_obj:
            _LOG.warning(f'Tenant: {tenant} not found')
            return
        _get_parent_id = key or self._parent_id_from_tenant
        pid = _get_parent_id(tenant)
        if not pid:
            _LOG.warning(f'Tenant does not contain management '
                         f'parent id.')
            return
        return self.get_by_parent(pid, tenant_obj)

    def get_by_parent(self, parent: Union[Parent, str],
                      tenant: Optional[Tenant] = None,
                      ) -> Optional[Credentials]:
        parent_obj = self._assure_parent_obj(parent)
        if not parent_obj:
            _LOG.warning(f'Parent: {parent} not found')
            return
        aid = parent_obj.application_id
        if not aid:
            _LOG.warning(f'Parent {parent} does not contain application id.')
            return
        return self.get_by_application(aid, tenant)

    def get_by_application(self, application: Union[Application, str],
                           tenant: Optional[Tenant] = None,
                           ) -> Optional[Credentials]:
        """
        Retrieves everything it can from application. Currently only
        some types are implemented
        """
        application_obj = self._assure_application_obj(application)
        if not application_obj:
            _LOG.warning(f'Application: {application} not found')
            return
        getter = self.application_type_to_getter.get(application_obj.type)
        if not getter:
            _LOG.warning(
                f'Not available application type {application_obj.type}')
            return
        return getter(application_obj, tenant)

    @cached_property
    def application_type_to_getter(
            self) -> Dict[
        str, Callable[[Application, Optional[Tenant]], Credentials]]:
        """
        Method must have application as input
        """
        return {
            ApplicationType.AZURE_CREDENTIALS: self._get_azure_credentials,
            ApplicationType.AZURE_CERTIFICATE: self._get_azure_certificate,
            ApplicationType.AWS_CREDENTIALS: self._get_aws_credentials,
            ApplicationType.AWS_ROLE: self._get_aws_credentials_from_role,
            ApplicationType.GCP_SERVICE_ACCOUNT: self._get_gcp_credentials,
            ApplicationType.GCP_COMPUTE_ACCOUNT: self._get_gcp_credentials,
            ApplicationType.RABBITMQ: self._get_rabbitmq_credentials,
        }

    def _get_aws_credentials_from_role(self, application: Application,
                                       tenant: Optional[Tenant] = None,
                                       ) -> Optional[AWSCredentials]:
        meta = AWSRoleApplicationMeta.from_dict(application.meta.as_dict())
        role = meta.roleName
        account_id = tenant.project if tenant else meta.accountNumber
        if not role:
            return
        role_arn = self._sts_service.build_role_arn(role, account_id)
        try:
            creds = self._sts_service.assume_role(
                role_arn=role_arn,
                duration=3600,
                role_session_name=f'credentials-service-{str(int(time()))}'
            )
            return AWSCredentials(
                AWS_ACCESS_KEY_ID=creds['aws_access_key_id'],
                AWS_SECRET_ACCESS_KEY=creds['aws_secret_access_key'],
                AWS_SESSION_TOKEN=creds['aws_session_token'],
                AWS_DEFAULT_REGION=self._default_aws_region()
            )
        except (ClientError, ConnectionAbortedError) as e:
            _LOG.warning(f'Error occurred trying to assume role: {role}. {e}')
            return

    def _get_aws_credentials(self, application: Application,
                             tenant: Optional[Tenant] = None,
                             ) -> Optional[AWSCredentials]:
        if not application.secret:
            _LOG.warning(f'Application {application.application_id} does not '
                         f'contain secret')
            return
        secret = self._ssm_service.get_parameter(application.secret)
        if not secret:
            _LOG.warning(f'Secret {application.secret} exists in application,'
                         f' but not in SSM')
            return
        secret = AWSCredentialsApplicationSecret.from_dict(secret)
        return AWSCredentials(
            AWS_ACCESS_KEY_ID=secret.accessKeyId,
            AWS_SECRET_ACCESS_KEY=secret.secretAccessKey,
            AWS_SESSION_TOKEN=secret.sessionToken,
            AWS_DEFAULT_REGION=secret.defaultRegion or self._default_aws_region()
        )

    def _get_azure_credentials(self, application: Application,
                               tenant: Optional[Tenant] = None,
                               ) -> Optional[AZURECredentials]:
        if not application.secret:
            _LOG.warning(f'Application {application.application_id} does not '
                         f'contain secret')
            return
        secret = self._ssm_service.get_parameter(application.secret)
        if not secret:
            _LOG.warning(f'Secret {application.secret} exists in application,'
                         f' but not in SSM')
            return
        if isinstance(secret, str):
            meta = AZURECredentialsApplicationMeta.from_dict(
                application.meta.as_dict()
            )
            tenant_id = meta.tenantId
            client_id = meta.clientId
            api_key = secret
        else:  # isinstance(secret, dict)
            secret = AZURECredentialsApplicationSecret.from_dict(secret)
            tenant_id = secret.tenant_id
            client_id = secret.client_id
            api_key = secret.api_key
        return AZURECredentials(
            AZURE_TENANT_ID=tenant_id,
            AZURE_CLIENT_ID=client_id,
            AZURE_CLIENT_SECRET=api_key,
            AZURE_SUBSCRIPTION_ID=tenant.project if tenant else None
        )

    def _get_azure_certificate(self, application: Application,
                               tenant: Optional[Tenant] = None,
                               ) -> Optional[AZURECertificate]:
        if not application.secret:
            _LOG.warning(f'Application {application.application_id} does not '
                         f'contain secret')
            return
        secret = self._ssm_service.get_parameter(application.secret)
        if not secret:
            _LOG.warning(f'Secret {application.secret} exists in application,'
                         f' but not in SSM')
            return
        meta = AZURECertificateApplicationMeta.from_dict(
            application.meta.as_dict()
        )
        secret = AZURECertificateApplicationSecret.from_dict(secret)
        with tempfile.NamedTemporaryFile('wb', delete=False) as fp:
            fp.write(base64.b64decode(secret.certificate_base64))

        return AZURECertificate(
            AZURE_TENANT_ID=meta.tenantId,
            AZURE_CLIENT_ID=meta.clientId,
            AZURE_CLIENT_CERTIFICATE_PATH=Path(fp.name),
            AZURE_CLIENT_CERTIFICATE_PASSWORD=secret.certificate_password
        )

    def _get_gcp_credentials(self, application: Application,
                             tenant: Optional[Tenant] = None,
                             ) -> Optional[GOOGLECredentials]:
        if not application.secret:
            _LOG.warning(f'Application {application.application_id} does not '
                         f'contain secret')
            return
        secret = self._ssm_service.get_parameter(application.secret)
        if not secret:
            _LOG.warning(f'Secret {application.secret} exists in application,'
                         f' but not in SSM')
            return
        if not isinstance(secret, dict):  # it must be dict
            return
        meta = GCPServiceAccountApplicationMeta.from_dict(
            application.meta.as_dict()
        )
        project_id = meta.adminProjectId
        secret.setdefault(MA_SSM_PROJECT_ID, project_id)  # just in case
        with tempfile.NamedTemporaryFile('w', delete=False) as fp:
            json.dump(secret, fp)
        return GOOGLECredentials(
            GOOGLE_APPLICATION_CREDENTIALS=Path(fp.name),
            CLOUDSDK_CORE_PROJECT=project_id  # or secret['project_id'] ?
        )

    def _get_rabbitmq_credentials(self, application: Application,
                                  tenant: Optional[Tenant] = None
                                  ) -> Optional[RabbitMQCredentials]:
        if not application.secret:
            _LOG.warning(f'Application {application.application_id} does not '
                         f'contain secret')
            return
        secret = self._ssm_service.get_parameter(application.secret)
        if not secret:
            _LOG.warning(f'Secret {application.secret} exists in application,'
                         f' but not in SSM')
            return
        if not isinstance(secret, dict):  # it must be dict
            return
        meta = RabbitMQApplicationMeta.from_dict(application.meta.as_dict())
        secret = RabbitMQApplicationSecret.from_dict(secret)
        return RabbitMQCredentials(
            connection_url=secret.connection_url,
            sdk_secret_key=secret.sdk_secret_key,
            maestro_user=meta.maestro_user,
            rabbit_exchange=meta.rabbit_exchange,
            request_queue=meta.request_queue,
            response_queue=meta.response_queue,
            sdk_access_key=meta.sdk_access_key
        )
