from pynamodb.attributes import UnicodeAttribute, MapAttribute, \
    UTCDateTimeAttribute

from modular_sdk.models.pynamongo.models import BaseModel


class Heartbeat(BaseModel):
    class Meta:
        table_name = 'Heartbeats'

    component = UnicodeAttribute(hash_key=True)
    event_date = UTCDateTimeAttribute(range_key=True)
    health_check = MapAttribute(default=dict)
