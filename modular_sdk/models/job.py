from pynamodb.attributes import UnicodeAttribute, MapAttribute, \
    UTCDateTimeAttribute
from pynamodb.indexes import AllProjection

from pynamodb.indexes import GlobalSecondaryIndex
from modular_sdk.models.pynamongo.models import BaseModel


class JobStartedAtIndex(GlobalSecondaryIndex):
    class Meta:
        index_name = "job-started_at-index"
        read_capacity_units = 1
        write_capacity_units = 1
        projection = AllProjection()

    job = UnicodeAttribute(hash_key=True)
    started_at = UnicodeAttribute(range_key=True)


class Job(BaseModel):
    class Meta:
        table_name = 'ModularJobs'

    job = UnicodeAttribute(hash_key=True)
    job_id = UnicodeAttribute(range_key=True)
    application = UnicodeAttribute()
    started_at = UnicodeAttribute()
    stopped_at = UnicodeAttribute(null=True)
    state = UnicodeAttribute()
    error_type = UnicodeAttribute(null=True)
    error_reason = UnicodeAttribute(null=True)
    meta = MapAttribute(default=dict)

    job_started_at_index = JobStartedAtIndex()
