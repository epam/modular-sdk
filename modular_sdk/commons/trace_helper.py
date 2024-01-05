from functools import wraps

from modular_sdk.modular import Modular
from modular_sdk.utils.operation_mode.generic import \
    ModularOperationModeManagerService
from modular_sdk.utils.runtime_tracer.generic import SegmentTracer, \
    ScheduledSegmentTracer


def __resolve_event(args, kwargs):
    if len(args) != 2:
        return kwargs['event']
    return args[0]


def __resolve_context(args, kwargs):
    if len(args) != 2:
        return kwargs['context']
    return args[1]


def tracer_decorator(is_scheduled=False, is_job=False, component=None):
    def real_wrapper(func):
        @wraps(func)
        def wrapper(*args, **kwargs):

            environment_service = Modular().environment_service()
            sqs_service = Modular().sqs_service()

            job_tracer = None
            request_id = None
            event = {}
            if is_job:
                from modular_sdk.utils.job_tracer.generic import \
                    ModularJobTracer
                operation_mode_service = ModularOperationModeManagerService()
                job_tracer = ModularJobTracer(
                    operation_mode_service=operation_mode_service,
                    environment_service=environment_service,
                    component=component
                )

                event = __resolve_event(args, kwargs)
                context = __resolve_context(args, kwargs)
                request_id = event.get('request_id') or context.aws_request_id

            if is_scheduled:
                runtime_tracer = ScheduledSegmentTracer(
                    sqs_service=sqs_service,
                    lambda_service=Modular().lambda_service(),
                    events_service=Modular().events_service(),
                    environment_service=environment_service
                )

            else:
                runtime_tracer = SegmentTracer(
                    sqs_service=sqs_service,
                    environment_service=environment_service
                )

            segment = runtime_tracer.start()
            if is_job:
                meta = None
                dry_run = event.get('dry_run')
                if dry_run in ('true', 't', 'y', 'yes') \
                        or (isinstance(dry_run, bool) and dry_run):
                    meta = {'message': 'Dry run mode enabled.'}
                job_tracer.start(job_id=request_id, meta=meta)
            try:
                from aws_xray_sdk.core import patch
                libs_to_patch = ('boto3', 'pynamodb')
                patch(libs_to_patch)
                response = func(*args, **kwargs)
            except Exception as e:
                if is_job:
                    job_tracer.fail(job_id=request_id, error=e)
                segment.error()
                raise e
            else:
                if is_job:
                    job_tracer.succeed(job_id=request_id, meta=response)
                segment.stop()
                return response

        return wrapper

    return real_wrapper
