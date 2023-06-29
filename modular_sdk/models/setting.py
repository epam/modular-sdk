from pynamodb.attributes import UnicodeAttribute

from modular_sdk.models.base_meta import BaseMeta
from modular_sdk.models.pynamodb_extension.base_model import DynamicAttribute
from modular_sdk.models.pynamodb_extension.base_role_access_model import \
    BaseRoleAccessModel


class Setting(BaseRoleAccessModel):
    class Meta(BaseMeta):
        table_name = 'Settings'

    name = UnicodeAttribute(hash_key=True, attr_name='s')
    value = DynamicAttribute(attr_name='v')
