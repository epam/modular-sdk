from pynamodb.attributes import UnicodeAttribute, MapAttribute, \
    UTCDateTimeAttribute

from modular_sdk.models.pynamodb_extension.base_role_access_model import \
    BaseRoleAccessModel
from modular_sdk.models.base_meta import BaseMeta


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
