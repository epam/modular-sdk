from datetime import datetime
from pynamodb.exceptions import DoesNotExist

from modular_sdk.commons import ModularException, RESPONSE_RESOURCE_NOT_FOUND_CODE, \
    RESPONSE_BAD_REQUEST_CODE, RESPONSE_FORBIDDEN_CODE
from modular_sdk.models.job import Job

from modular_sdk.commons.log_helper import get_logger
from modular_sdk.services.environment_service import EnvironmentService
from modular_sdk.utils.operation_mode.generic import \
    ModularOperationModeManagerService
from modular_sdk.utils.job_tracer.abstract import AbstractJobTracer

_LOG = get_logger('modular_sdk-job-tracer')


class ModularJobTracer(AbstractJobTracer):
    def __init__(self, operation_mode_service: ModularOperationModeManagerService,
                 environment_service: EnvironmentService):
        self.operation_mode_service = operation_mode_service
        self.environment_service = environment_service
        self.application = self.environment_service.application()
        self.component = self.environment_service.component()

    def start(self, job_id):
        _LOG.debug(f'Going to mark Job {self.component} and {job_id} id as '
                   f'started')

        # self.is_permitted_to_start()
        Job(
            job=self.component,
            job_id=job_id,
            application=self.application,
            state='RUNNING',
            started_at=datetime.utcnow(),
        ).save()

    def is_permitted_to_start(self):
        result = self.operation_mode_service.get_mode(
            application_name=self.application
        )
        if result.get('items', {})[0].get('mode', '') == 'LIVE':
            return
        unappropriated_mode_message = f'{self.application} can not be traced ' \
                                      f'due to unappropriated mode'
        _LOG.error(unappropriated_mode_message)
        raise ModularException(
            code=RESPONSE_FORBIDDEN_CODE,
            content=unappropriated_mode_message
        )

    @staticmethod
    def __get_job(job, job_id):
        try:
            job_item = Job.get_nullable(hash_key=job, range_key=job_id)
        except DoesNotExist:
            job_not_exist_message = f'Job with {job} name and {job_id} ' \
                                    f'id does not exists'
            _LOG.error(job_not_exist_message)
            raise ModularException(
                code=RESPONSE_RESOURCE_NOT_FOUND_CODE,
                content=job_not_exist_message
            )
        if job_item.state in ['SUCCESS', 'FAIL']:
            job_invalid_state_message = f'Job with {job_id} id and ' \
                                        f'{job} name with state ' \
                                        f'not equal to RUNNING can not be ' \
                                        f'changed'

            _LOG.error(job_invalid_state_message)
            raise ModularException(
                code=RESPONSE_BAD_REQUEST_CODE,
                content=job_invalid_state_message
            )
        return job_item

    def fail(self, job_id, error):
        _LOG.debug(f'Going to mark Job {self.component} and {job_id} id as '
                   f'failed')

        job_item = self.__get_job(job=self.component, job_id=job_id)

        job_item.stopped_at = datetime.utcnow()
        job_item.state = 'FAIL'
        job_item.error_type = error.__class__.__name__
        job_item.error_reason = error.__str__()
        job_item.save()

    def succeed(self, job_id):
        _LOG.debug(f'Going to mark Job {self.component} and {job_id} id as '
                   f'succeeded')
        job_item = self.__get_job(job=self.component, job_id=job_id)

        job_item.stopped_at = datetime.utcnow()
        job_item.state = 'SUCCESS'
        job_item.save()

    def track_error(self):
        # todo submit error to aggregation pipeline
        pass
