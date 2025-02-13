from enum import Enum
import os

HTTP_ATTR, HTTPS_ATTR = 'HTTP', 'HTTPS'

ASSUMES_ROLE_SESSION_NAME = 'modular'

MODULAR_AWS_ACCESS_KEY_ID_ENV = 'modular_aws_access_key_id'
MODULAR_AWS_SECRET_ACCESS_KEY_ENV = 'modular_aws_secret_access_key'
MODULAR_AWS_SESSION_TOKEN_ENV = 'modular_aws_session_token'
MODULAR_AWS_CREDENTIALS_EXPIRATION_ENV = 'modular_aws_credentials_expiration'

_SENTINEL = object()


class Env(str, Enum):
    """
    Abstract enumeration class for holding environment variables
    """
    default: str | None

    def __new__(cls, value: str, default: str | None = None):
        """
        All environment variables and optionally their default values.
        Since envs always have string type the default value also should be
        of string type and then converted to the necessary type in code.
        There is no default value if not specified (default equal to unset)
        """
        obj = str.__new__(cls, value)
        obj._value_ = value

        obj.default = default
        return obj

    def get(self, default=_SENTINEL) -> str | None:
        if default is _SENTINEL:
            default = self.default
        if default is not None:
            default = str(default)
        return os.environ.get(self.value, default)

    def set(self, val: str | None):
        if val is None:
            os.environ.pop(self.value, None)
        else:
            os.environ[self.value] = str(val)

    # OLD ones
    OLD_SERVICE_MODE = 'modular_service_mode'
    OLD_MONGO_USER = 'modular_mongo_user'
    OLD_MONGO_PASSWORD = 'modular_mongo_password'
    OLD_MONGO_URL = 'modular_mongo_url'
    OLD_MONGO_URI = 'modular_mongo_uri'
    OLD_MONGO_DB_NAME = 'modular_mongo_db_name'
    OLD_MONGO_SRV = 'modular_mongo_srv'
    OLD_ASSUME_ROLE_ARN = 'modular_assume_role_arn'  # may be multiple split by ,

    OLD_MODULAR_AWS_REGION = 'MODULAR_AWS_REGION'  # used for cross account models access
    OLD_INNER_CACHE_TTL_SECONDS = 'INNER_CACHE_TTL_SECONDS', '600'

    # TODO they are NOT used currently. We should add their support to
    #  code but make a gradual transition from the old ones so that both
    #  old and new are supported for some time.
    # NEW ones
    SERVICE_MODE = 'MODULAR_SDK_SERVICE_MODE'
    MONGO_USER = 'MODULAR_SDK_MONGO_USER'
    MONGO_PASSWORD = 'MODULAR_SDK_MONGO_PASSWORD'
    MONGO_URL = 'MODULAR_SDK_MONGO_URL'
    MONGO_URI = 'MODULAR_SDK_MONGO_URI'
    MONGO_DB_NAME = 'MODULAR_SDK_MONGO_DB_NAME'
    ASSUME_ROLE_ARN = 'MODULAR_SDK_ASSUME_ROLE_ARN'
    ASSUME_ROLE_REGION = 'MODULAR_SDK_ASSUME_ROLE_REGION'
    INNER_CACHE_TTL_SECONDS = 'MODULAR_SDK_INNER_CACHE_TTL_SECONDS', '300'

    # these below are used
    AWS_REGION = 'AWS_REGION'
    AWS_DEFAULT_REGION = 'AWS_DEFAULT_REGION'
    LOG_LEVEL = 'MODULAR_SDK_LOG_LEVEL', 'INFO'


REGION_ENV = Env.AWS_REGION.value
DEFAULT_REGION_ENV = Env.AWS_DEFAULT_REGION.value
MODULAR_REGION_ENV = Env.OLD_MODULAR_AWS_REGION.value

MODULAR_SERVICE_MODE_ENV = Env.OLD_SERVICE_MODE.value
SERVICE_MODE_DOCKER = 'docker'
SERVICE_MODE_SAAS = 'saas'

PARAM_MONGO_USER = Env.OLD_MONGO_USER.value
PARAM_MONGO_PASSWORD = Env.OLD_MONGO_PASSWORD.value
PARAM_MONGO_URL = Env.OLD_MONGO_URL.value
PARAM_MONGO_URI = Env.OLD_MONGO_URI.value
PARAM_MONGO_DB_NAME = Env.OLD_MONGO_DB_NAME.value
PARAM_MONGO_SRV = Env.OLD_MONGO_SRV.value
PARAM_ASSUME_ROLE_ARN = Env.OLD_ASSUME_ROLE_ARN.value

ENV_INNER_CACHE_TTL_SECONDS = Env.OLD_INNER_CACHE_TTL_SECONDS.value
DEFAULT_INNER_CACHE_TTL_SECONDS: int = int(Env.OLD_INNER_CACHE_TTL_SECONDS.default)


class ParentType(str, Enum):
    AWS_ATHENA = 'AWS_ATHENA'
    AZURE_AD_SSO = 'AZURE_AD_SSO'
    GCP_SECURITY = 'GCP_SECURITY'
    AWS_MANAGEMENT = 'AWS_MANAGEMENT'
    GCP_MANAGEMENT = 'GCP_MANAGEMENT'
    AZURE_RATE_CARDS = 'AZURE_RATE_CARDS'
    AZURE_MANAGEMENT = 'AZURE_MANAGEMENT'
    AWS_COST_EXPLORER = 'AWS_COST_EXPLORER'
    AZURE_CSP_BILLING = 'AZURE_CSP_BILLING'
    AZURE_CSP_PARTNER = 'AZURE_CSP_PARTNER'
    AZURE_USAGE_DETAILS = 'AZURE_USAGE_DETAILS'
    GCP_BILLING_SERVICE = 'GCP_BILLING_SERVICE'
    AZURE_ENTERPRISE_BILLING = 'AZURE_ENTERPRISE_BILLING'
    CUSTODIAN = 'CUSTODIAN'
    CUSTODIAN_ACCESS = 'CUSTODIAN_ACCESS'
    CUSTODIAN_LICENSES = 'CUSTODIAN_LICENSES'
    RIGHTSIZER_PARENT = 'RIGHTSIZER'
    RIGHTSIZER_LICENSES_PARENT = 'RIGHTSIZER_LICENSES'
    RIGHTSIZER_SIEM_DEFECT_DOJO = 'RIGHTSIZER_SIEM_DEFECT_DOJO'
    CUSTODIAN_SIEM_DEFECT_DOJO = 'CUSTODIAN_SIEM_DEFECT_DOJO'
    PLATFORM_K8S = 'PLATFORM_K8S'
    GCP_CHRONICLE_INSTANCE = 'GCP_CHRONICLE_INSTANCE'

    @classmethod
    def iter(cls):
        """
        Iterates over values, not enum items
        """
        return map(lambda x: x.value, cls)


# backward compatibility, use enum instead
AWS_ATHENA = ParentType.AWS_ATHENA.value
AZURE_AD_SSO = ParentType.AZURE_AD_SSO.value
GCP_SECURITY = ParentType.GCP_SECURITY.value
AWS_MANAGEMENT = ParentType.AWS_MANAGEMENT.value
GCP_MANAGEMENT = ParentType.GCP_MANAGEMENT.value
AZURE_RATE_CARDS = ParentType.AZURE_RATE_CARDS.value
AZURE_MANAGEMENT = ParentType.AZURE_MANAGEMENT.value
AWS_COST_EXPLORER = ParentType.AWS_COST_EXPLORER.value
AZURE_CSP_BILLING = ParentType.AZURE_CSP_BILLING.value
AZURE_CSP_PARTNER = ParentType.AZURE_CSP_PARTNER.value
AZURE_USAGE_DETAILS = ParentType.AZURE_USAGE_DETAILS.value
GCP_BILLING_SERVICE = ParentType.GCP_BILLING_SERVICE.value
AZURE_ENTERPRISE_BILLING = ParentType.AZURE_ENTERPRISE_BILLING.value
CUSTODIAN_TYPE = ParentType.CUSTODIAN.value
CUSTODIAN_ACCESS_TYPE = ParentType.CUSTODIAN_ACCESS.value
CUSTODIAN_LICENSES_TYPE = ParentType.CUSTODIAN_LICENSES.value
RIGHTSIZER_PARENT_TYPE = ParentType.RIGHTSIZER_PARENT.value
RIGHTSIZER_LICENSES_PARENT_TYPE = ParentType.RIGHTSIZER_LICENSES_PARENT.value
RIGHTSIZER_SIEM_DEFECT_DOJO_TYPE = ParentType.RIGHTSIZER_SIEM_DEFECT_DOJO.value
CUSTODIAN_SIEM_DEFECT_DOJO_TYPE = ParentType.CUSTODIAN_SIEM_DEFECT_DOJO.value
ALL_PARENT_TYPES = list(ParentType.iter())


class Cloud(str, Enum):
    AZURE = 'AZURE'
    YANDEX = 'YANDEX'
    GOOGLE = 'GOOGLE'
    AWS = 'AWS'
    OPENSTACK = 'OPEN_STACK'
    CSA = 'CSA'
    HWU = 'HARDWARE'
    ENTERPRISE = 'ENTERPRISE'
    EXOSCALE = 'EXOSCALE'
    WORKSPACE = 'WORKSPACE'
    AOS = 'AOS'
    VSPHERE = 'VSPHERE'
    VMWARE = 'VMWARE'  # VCloudDirector group
    NUTANIX = 'NUTANIX'

    @classmethod
    def iter(cls):
        """
        Iterates over values, not enum items
        """
        return map(lambda x: x.value, cls)


# todo deprecated
AZURE_CLOUD = Cloud.AZURE.value
YANDEX_CLOUD = Cloud.YANDEX.value
GOOGLE_CLOUD = Cloud.GOOGLE.value
AWS_CLOUD = Cloud.AWS.value
OPENSTACK_CLOUD = Cloud.OPENSTACK.value
CSA_CLOUD = Cloud.CSA.value
HWU_CLOUD = Cloud.HWU.value
ENTERPRISE_CLOUD = Cloud.ENTERPRISE.value
EXOSCALE_CLOUD = Cloud.EXOSCALE.value
WORKSPACE_CLOUD = Cloud.WORKSPACE.value
AOS_CLOUD = Cloud.AOS.value
VSPHERE_CLOUD = Cloud.VSPHERE.value
VMWARE_CLOUD = Cloud.VMWARE.value  # VCloudDirector group
NUTANIX_CLOUD = Cloud.NUTANIX.value

CLOUD_SHORT_LONG_NAME_MAPPING = {
    Cloud.AZURE.value: 'AZ',
    Cloud.GOOGLE.value: 'GGL',
    Cloud.HWU.value: 'HW',
    Cloud.EXOSCALE.value: 'EXO',
    Cloud.ENTERPRISE.value: 'ENT',
}

CLOUD_PROVIDERS = list(Cloud.iter())


class ApplicationType(str, Enum):
    AWS_ROLE = 'AWS_ROLE'
    AWS_CREDENTIALS = 'AWS_CREDENTIALS'
    AZURE_CREDENTIALS = 'AZURE_CREDENTIALS'
    AZURE_CERTIFICATE = 'AZURE_CERTIFICATE'
    AZURE_ENROLMENT = 'AZURE_ENROLMENT'
    GCP_COMPUTE_ACCOUNT = 'GCP_COMPUTE_ACCOUNT'
    GCP_SERVICE_ACCOUNT = 'GCP_SERVICE_ACCOUNT'
    CUSTODIAN = 'CUSTODIAN'
    CUSTODIAN_LICENSES = 'CUSTODIAN_LICENSES'
    RIGHTSIZER = 'RIGHTSIZER'
    RIGHTSIZER_LICENSES = 'RIGHTSIZER_LICENSES'
    RABBITMQ = 'RABBITMQ'
    DEFECT_DOJO = 'DEFECT_DOJO'
    K8S_KUBE_CONFIG = 'K8S_KUBE_CONFIG'
    GCP_CHRONICLE_INSTANCE = 'GCP_CHRONICLE_INSTANCE'
    SEP_SANDBOX_AWS = 'SEP_SANDBOX_AWS'

    @classmethod
    def iter(cls):
        """
        Iterates over values, not enum items
        """
        return map(lambda x: x.value, cls)


# backward compatibility
AWS_ROLE = ApplicationType.AWS_ROLE.value
AWS_CREDENTIALS = ApplicationType.AWS_CREDENTIALS.value
AZURE_CREDENTIALS = ApplicationType.AZURE_CREDENTIALS.value
AZURE_CERTIFICATE = ApplicationType.AZURE_CERTIFICATE.value
AZURE_ENROLMENT = ApplicationType.AZURE_ENROLMENT.value
GCP_COMPUTE_ACCOUNT = ApplicationType.GCP_COMPUTE_ACCOUNT.value
GCP_SERVICE_ACCOUNT = ApplicationType.GCP_SERVICE_ACCOUNT.value
# CUSTODIAN_TYPE = 'CUSTODIAN'  # declared in parents
# CUSTODIAN_LICENSES_TYPE = 'CUSTODIAN_LICENSES'
RIGHTSIZER_TYPE = ApplicationType.RIGHTSIZER.value
RIGHTSIZER_LICENSES_TYPE = ApplicationType.RIGHTSIZER_LICENSES.value
RABBITMQ_TYPE = ApplicationType.RABBITMQ.value
DEFECT_DOJO_TYPE = ApplicationType.DEFECT_DOJO.value
SEP_SANDBOX_AWS_TYPE = ApplicationType.SEP_SANDBOX_AWS.value

AVAILABLE_APPLICATION_TYPES = list(ApplicationType.iter())

# environment service
ENVS_TO_HIDE = set()
HIDDEN_ENV_PLACEHOLDER = '****'

# Tenant parent map types - probably deprecate
TENANT_PARENT_MAP_BILLING_TYPE = 'BILLING'
TENANT_PARENT_MAP_MANAGEMENT_TYPE = 'MANAGEMENT'
TENANT_PARENT_MAP_CUSTODIAN_TYPE = CUSTODIAN_TYPE
TENANT_PARENT_MAP_CUSTODIAN_ACCESS_TYPE = CUSTODIAN_ACCESS_TYPE
TENANT_PARENT_MAP_CUSTODIAN_LICENSES_TYPE = CUSTODIAN_LICENSES_TYPE
TENANT_PARENT_MAP_RIGHTSIZER_TYPE = 'RIGHTSIZER'
TENANT_PARENT_MAP_RIGHTSIZER_LICENSES_TYPE = 'RIGHTSIZER_LICENSES'
TENANT_PARENT_MAP_RIGHTSIZER_SIEM_DEFECT_DOJO_TYPE = (
    RIGHTSIZER_SIEM_DEFECT_DOJO_TYPE)
TENANT_PARENT_MAP_CUSTODIAN_SIEM_DEFECT_DOJO_TYPE = (
    CUSTODIAN_SIEM_DEFECT_DOJO_TYPE)

ALLOWED_TENANT_PARENT_MAP_KEYS = (
    TENANT_PARENT_MAP_BILLING_TYPE,
    TENANT_PARENT_MAP_MANAGEMENT_TYPE,
    TENANT_PARENT_MAP_CUSTODIAN_TYPE,
    TENANT_PARENT_MAP_RIGHTSIZER_TYPE,
    TENANT_PARENT_MAP_RIGHTSIZER_LICENSES_TYPE,
    TENANT_PARENT_MAP_RIGHTSIZER_SIEM_DEFECT_DOJO_TYPE,
    TENANT_PARENT_MAP_CUSTODIAN_SIEM_DEFECT_DOJO_TYPE,
    TENANT_PARENT_MAP_CUSTODIAN_LICENSES_TYPE,
    TENANT_PARENT_MAP_CUSTODIAN_ACCESS_TYPE
)

DEFAULT_AWS_REGION = 'us-east-1'

# native cloud credentials envs
# AWS
ENV_AWS_ACCESS_KEY_ID = 'AWS_ACCESS_KEY_ID'
ENV_AWS_SECRET_ACCESS_KEY = 'AWS_SECRET_ACCESS_KEY'
ENV_AWS_SESSION_TOKEN = 'AWS_SESSION_TOKEN'
ENV_AWS_DEFAULT_REGION = 'AWS_DEFAULT_REGION'

# AZURE
ENV_AZURE_TENANT_ID = 'AZURE_TENANT_ID'
ENV_AZURE_SUBSCRIPTION_ID = 'AZURE_SUBSCRIPTION_ID'
ENV_AZURE_CLIENT_ID = 'AZURE_CLIENT_ID'
ENV_AZURE_CLIENT_SECRET = 'AZURE_CLIENT_SECRET'
ENV_AZURE_CLIENT_CERTIFICATE_PATH = 'AZURE_CLIENT_CERTIFICATE_PATH'
ENV_AZURE_CLIENT_CERTIFICATE_PASSWORD = 'AZURE_CLIENT_CERTIFICATE_PASSWORD'

# GOOGLE
ENV_GOOGLE_APPLICATION_CREDENTIALS = 'GOOGLE_APPLICATION_CREDENTIALS'
ENV_CLOUDSDK_CORE_PROJECT = 'CLOUDSDK_CORE_PROJECT'

# KUBERNETES
ENV_KUBECONFIG = 'KUBECONFIG'

COMPOUND_KEYS_SEPARATOR = '#'


class ParentScope(str, Enum):
    ALL = 'ALL'
    DISABLED = 'DISABLED'
    SPECIFIC = 'SPECIFIC'

    @classmethod
    def iter(cls):
        """
        Iterates over values, not enum items
        """
        return map(lambda x: x.value, cls)


JOB_SUCCESS_STATE = 'SUCCESS'
JOB_FAIL_STATE = 'FAIL'
JOB_RUNNING_STATE = 'RUNNING'

PLAIN_CONTENT_TYPE = 'text/plain'
SUCCESS_STATUS = 'SUCCESS'
ERROR_STATUS = 'FAILED'
RESULTS = 'results'
DATA = 'data'
