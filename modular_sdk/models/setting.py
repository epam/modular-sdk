from pynamodb.attributes import UnicodeAttribute

from modular_sdk.models.base_meta import BaseMeta
from modular_sdk.models.pynamongo.models import BaseModel

from modular_sdk.models.pynamongo.attributes import DynamicAttribute


class Setting(BaseModel):
    class Meta(BaseMeta):
        table_name = 'Settings'

    name = UnicodeAttribute(hash_key=True, attr_name='s')
    value = DynamicAttribute(attr_name='v')
