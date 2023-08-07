from pynamodb.attributes import UnicodeAttribute, MapAttribute, \
    UTCDateTimeAttribute
from pynamodb.indexes import AllProjection

from modular_sdk.models.pynamodb_extension.base_model import BaseGSI
from modular_sdk.models.pynamodb_extension.base_role_access_model import \
    BaseRoleAccessModel
from modular_sdk.models.base_meta import BaseMeta


class JobStartedAtIndex(BaseGSI):
    class Meta:
        index_name = "job-started_at-index"
        read_capacity_units = 1
        write_capacity_units = 1
        projection = AllProjection()

    job = UnicodeAttribute(hash_key=True)
    started_at = UnicodeAttribute(range_key=True)


class Job(BaseRoleAccessModel):
    class Meta(BaseMeta):
        table_name = 'ModularJobs'

    job = UnicodeAttribute(hash_key=True)
    job_id = UnicodeAttribute(range_key=True)
    application = UnicodeAttribute()
    started_at = UTCDateTimeAttribute()
    stopped_at = UTCDateTimeAttribute(null=True)
    state = UnicodeAttribute()
    error_type = UnicodeAttribute(null=True)
    error_reason = UnicodeAttribute(null=True)
    meta = MapAttribute(default=dict)

    job_started_at_index = JobStartedAtIndex()
