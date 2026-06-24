from pynamodb.attributes import (
    BooleanAttribute,
    ListAttribute,
    MapAttribute,
    NumberAttribute,
    UnicodeAttribute,
)

from modular_sdk.models.base_meta import BaseMeta, TABLES_PREFIX
from modular_sdk.models.pynamongo.attributes import DynamicAttribute, \
    M3BooleanAttribute
from modular_sdk.models.pynamongo.models import ModularBaseModel

EMAIL = 'e'
CUSTOMER_ID = 'c'
ALLOWED_TENANTS = 't'
ROLE = 'r'
IDENTITY_ID = 'idenId'
TYPE = 'tp'
DIDEVICE_INFO = 'di'
UNIQUE_USER_ID = 'uusid'
USER_NAME = 'usn'
DISPLAY_EMAIL = 'diem'
PROFILE_PHOTO_URL = 'pphurl'
ACTIVATION = 'act'
EXPIRATION = 'exp'
LAST_LOGIN_DATE = 'lld'
LAST_UPDATE_DATE = 'lud'
BLOCKED = 'b'
USER_ADDITIONAL_PARAMS = 'uad'
JMX = 'jmx'
JMX_PERMISSIONS = 'jmxp'
TENANT_MAPPINGS = 'tm'
TENANT_MAPPING_EXPIRATION = 'expiration'
TENANT_APPROVAL = 'tenantApproval'
ADMIN_SECRET_KEY = 'ask'
ADMIN_FOR_CUSTOMERS = 'ca'
CUSTOM_PLACEHOLDER = 'cp'
API_USER = 'api'
API_RATE_LIMIT = 'apirl'
DESCRIPTION = 'des'
BLOCK_REASON = 'br'
BLOCK_INITIATOR = 'bi'
BLOCK_EXPIRATION = 'be'

MODULAR_USERS_TABLE_NAME = 'Users'


class TenantMapping(MapAttribute):
    tenant = UnicodeAttribute(attr_name='tenant')
    positions = ListAttribute(
        of=UnicodeAttribute,
        attr_name='positions',
        default=list,
    )
    expiration = NumberAttribute(
        attr_name=TENANT_MAPPING_EXPIRATION,
        null=True,
    )
    tenant_approval = MapAttribute(
        attr_name=TENANT_APPROVAL,
        default=dict,
        null=True,
    )


class User(ModularBaseModel):
    class Meta(BaseMeta):
        table_name = f'{TABLES_PREFIX}{MODULAR_USERS_TABLE_NAME}'

    email = UnicodeAttribute(hash_key=True, attr_name=EMAIL)
    customer_id = UnicodeAttribute(attr_name=CUSTOMER_ID)
    allowed_tenants = ListAttribute(
        attr_name=ALLOWED_TENANTS,
        default=list,
    )
    role = UnicodeAttribute(attr_name=ROLE, null=True)
    identity_id = UnicodeAttribute(attr_name=IDENTITY_ID, null=True)
    type = UnicodeAttribute(attr_name=TYPE, null=True)
    didevice_info = MapAttribute(
        attr_name=DIDEVICE_INFO,
        default=dict,
        null=True
    )
    unique_user_id = UnicodeAttribute(attr_name=UNIQUE_USER_ID, null=True)
    user_name = UnicodeAttribute(attr_name=USER_NAME)
    display_email = UnicodeAttribute(attr_name=DISPLAY_EMAIL)
    profile_photo_url = UnicodeAttribute(
        attr_name=PROFILE_PHOTO_URL,
        null=True,
    )
    activation = NumberAttribute(attr_name=ACTIVATION, null=True)
    expiration = NumberAttribute(attr_name=EXPIRATION, null=True)
    last_login_date = NumberAttribute(attr_name=LAST_LOGIN_DATE, null=True)
    last_update_date = NumberAttribute(attr_name=LAST_UPDATE_DATE, null=True)
    blocked = M3BooleanAttribute(attr_name=BLOCKED, null=True)
    user_additional_params = DynamicAttribute(
        attr_name=USER_ADDITIONAL_PARAMS,
        null=True,
    )
    jmx = UnicodeAttribute(attr_name=JMX, null=True)
    jmx_permissions = UnicodeAttribute(attr_name=JMX_PERMISSIONS, null=True)
    tenant_mappings = ListAttribute(
        of=TenantMapping,
        attr_name=TENANT_MAPPINGS,
        default=list,
    )
    admin_secret_key = UnicodeAttribute(attr_name=ADMIN_SECRET_KEY, null=True)
    admin_for_customers = ListAttribute(
        attr_name=ADMIN_FOR_CUSTOMERS,
        default=list,
    )
    custom_placeholder = UnicodeAttribute(
        attr_name=CUSTOM_PLACEHOLDER,
        null=True,
    )
    api_user = BooleanAttribute(attr_name=API_USER, null=True)
    api_rate_limit = NumberAttribute(attr_name=API_RATE_LIMIT, null=True)
    description = UnicodeAttribute(attr_name=DESCRIPTION, null=True)
    block_reason = UnicodeAttribute(attr_name=BLOCK_REASON, null=True)
    block_initiator = UnicodeAttribute(attr_name=BLOCK_INITIATOR, null=True)
    block_expiration = NumberAttribute(attr_name=BLOCK_EXPIRATION, null=True)
