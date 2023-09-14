from typing import Optional, List
from pynamodb.exceptions import DoesNotExist

from modular_sdk.commons import generate_id
from modular_sdk.commons import ModularException, \
    RESPONSE_RESOURCE_NOT_FOUND_CODE
from modular_sdk.commons.log_helper import get_logger
from modular_sdk.models.job import Job

_LOG = get_logger(__name__)


class JobService:
    @staticmethod
    def create(job, job_id, application, started_at, state, 
               stopped_at: Optional[dict] = None,
               error_type: Optional[str] = None, 
               error_reason: Optional[str] = None, 
               meta: Optional[dict] = None) -> Job:
        job_id = job_id or generate_id()
        return Job(job=job, job_id=job_id, application=application, 
            started_at=started_at, state=state, stopped_at=stopped_at, 
            error_type=error_type, error_reason=error_reason, meta=meta)
        
    @staticmethod
    def get_by_id(job: str, job_id: str) -> Optional[Job]:
        try:
            job_item = Job.get_nullable(hash_key=job, range_key=job_id)
        except DoesNotExist:
            job_does_not_exist_message = f'Job with {job} name and {job_id} ' \
                                    f'id does not exists'
            _LOG.error(job_does_not_exist_message)
            raise ModularException(
                code=RESPONSE_RESOURCE_NOT_FOUND_CODE,
                content=job_does_not_exist_message
            )
        return job_item

    @staticmethod
    def list(job) -> List[Job]:
        return list(Job.job_started_at_index.query(hash_key=job))

    @staticmethod
    def save(job: Job):
        job.save()

    @staticmethod
    def update(job: Job, started_at: Optional[str] = None, 
               state: Optional[str] = None, 
               stopped_at: Optional[dict] = None,
               error_type: Optional[str] = None, 
               error_reason: Optional[str] = None, 
               meta: Optional[dict] = None):
        job.update(
            actions=[
                Job.started_at.set(started_at or job.started_at),
                Job.state.set(state or job.state),
                Job.stopped_at.set(stopped_at or job.stopped_at),
                Job.error_type.set(error_type or job.error_type),
                Job.error_reason.set(error_reason or job.error_reason),
                Job.meta.set(meta or job.meta)
            ]
        )
        

    @staticmethod
    def get_dto(Job: Job) -> dict:
        return Job.get_json()
