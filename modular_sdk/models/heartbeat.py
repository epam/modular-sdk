from pynamodb.attributes import UnicodeAttribute, MapAttribute, \
    UTCDateTimeAttribute

from modular_sdk.models.pynamodb_extension.base_role_access_model import \
    BaseRoleAccessModel
from modular_sdk.models.base_meta import BaseMeta


class Heartbeat(BaseRoleAccessModel):
    class Meta(BaseMeta):
        table_name = 'Heartbeats'

    component = UnicodeAttribute(hash_key=True)
    event_date = UTCDateTimeAttribute(range_key=True)
    health_check = MapAttribute(default=dict)
