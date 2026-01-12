from pynamodb.attributes import UnicodeAttribute, UTCDateTimeAttribute, \
    ListAttribute, MapAttribute

from modular_sdk.models.pynamongo.models import ModularBaseModel


class OperationMode(ModularBaseModel):
    class Meta:
        table_name = "OperationMode"
    application = UnicodeAttribute(hash_key=True)
    mode = UnicodeAttribute()
    last_update_date = UTCDateTimeAttribute()
    applied_by = UnicodeAttribute()
    description = UnicodeAttribute(null=True)
    meta = MapAttribute(default=dict)
    testing_white_list = ListAttribute(default=list)
