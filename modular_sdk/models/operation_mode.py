from pynamodb.attributes import UnicodeAttribute, UTCDateTimeAttribute, \
    ListAttribute, MapAttribute

from modular_sdk.models.pynamongo.models import BaseModel

from modular_sdk.models.base_meta import BaseMeta


class OperationMode(BaseModel):
    class Meta(BaseMeta):
        table_name = "OperationMode"
    application = UnicodeAttribute(hash_key=True)
    mode = UnicodeAttribute()
    last_update_date = UTCDateTimeAttribute()
    applied_by = UnicodeAttribute()
    description = UnicodeAttribute(null=True)
    meta = MapAttribute(default=dict)
    testing_white_list = ListAttribute(default=list)
