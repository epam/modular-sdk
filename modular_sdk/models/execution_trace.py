from pynamodb.attributes import UnicodeAttribute, \
    UTCDateTimeAttribute, NumberAttribute
from pynamodb.indexes import AllProjection

from modular_sdk.models.pynamodb_extension.base_role_access_model import \
    BaseRoleAccessModel
from modular_sdk.models.base_meta import BaseMeta
from modular_sdk.models.pynamodb_extension.base_model import BaseGSI


class SegmentIndex(BaseGSI):
    class Meta(BaseMeta):
        index_name = 'segment-index'
        read_capacity_units = 1
        write_capacity_units = 1
        projection = AllProjection()

    segment = UnicodeAttribute(hash_key=True)
    execution_id = UnicodeAttribute(range_key=True)


class ExecutionTrace(BaseRoleAccessModel):
    class Meta(BaseMeta):
        table_name = 'ExecutionTraces'

    component = UnicodeAttribute(hash_key=True)
    execution_id = UnicodeAttribute(range_key=True)
    segment = UnicodeAttribute()
    segment_index = SegmentIndex()

    start_time = UTCDateTimeAttribute()
    end_time = UTCDateTimeAttribute()
    duration_seconds = NumberAttribute()
