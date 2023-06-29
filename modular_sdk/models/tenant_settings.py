from pynamodb.attributes import UnicodeAttribute, MapAttribute

from modular_sdk.models.base_meta import BaseMeta, TABLES_PREFIX
from modular_sdk.models.pynamodb_extension.base_role_access_model import \
    BaseRoleAccessModel
from modular_sdk.models.pynamodb_extension.base_model import BaseGSI
from pynamodb.indexes import AllProjection


TENANT_NAME = 't'
KEY = 'k'
VALUE = 'v'

MODULAR_TENANT_SETTINGS_TABLE_NAME = 'TenantSettings'


class KeyTenantNameIndex(BaseGSI):
    class Meta(BaseMeta):
        index_name = f'{KEY}-index'
        read_capacity_units = 1
        write_capacity_units = 1
        projection = AllProjection()

    key = UnicodeAttribute(hash_key=True, attr_name=KEY)


class TenantSettings(BaseRoleAccessModel):
    class Meta(BaseMeta):
        table_name = f'{TABLES_PREFIX}{MODULAR_TENANT_SETTINGS_TABLE_NAME}'

    tenant_name = UnicodeAttribute(hash_key=True, attr_name=TENANT_NAME)
    key = UnicodeAttribute(range_key=True, attr_name=KEY)

    value = MapAttribute(attr_name=VALUE, default=dict)
    key_tenant_name_index = KeyTenantNameIndex()
