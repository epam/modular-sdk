from pynamodb.attributes import UnicodeAttribute

from modular_sdk.models.base_meta import BaseMeta, TABLES_PREFIX
from modular_sdk.models.pynamodb_extension.base_model import DynamicAttribute
from modular_sdk.models.pynamodb_extension.base_role_access_model import (
    BaseRoleAccessModel,
)


CUSTOMER_NAME = 'cn'
KEY = 'k'
VALUE = 'v'

MODULAR_CUSTOMER_SETTINGS_TABLE_NAME = 'CustomerSettings'



class CustomerSettings(BaseRoleAccessModel):
    class Meta(BaseMeta):
        table_name = f'{TABLES_PREFIX}{MODULAR_CUSTOMER_SETTINGS_TABLE_NAME}'

    customer_name = UnicodeAttribute(hash_key=True, attr_name=CUSTOMER_NAME)
    key = UnicodeAttribute(range_key=True, attr_name=KEY)

    value = DynamicAttribute(null=True, attr_name=VALUE)

