from functools import wraps
from typing import (
    Mapping,
    Any,
    Protocol,
    cast,
    Optional,
    Tuple,
    Callable,
    TypeVar,
    ParamSpec,
)
from uuid import uuid4

from modular_sdk.modular import Modular
from modular_sdk.utils.operation_mode.generic import (
    ModularOperationModeManagerService,
)
from modular_sdk.utils.runtime_tracer.generic import (
    SegmentTracer,
    ScheduledSegmentTracer,
)

P = ParamSpec('P')
R = TypeVar('R')


class Context(Protocol):
    """Protocol for AWS Lambda context object.
    
    :ivar aws_request_id: AWS request ID
    :type aws_request_id: str
    """
    aws_request_id: str


def __get_arg_or_kwarg(
    args: Tuple[Any, ...],
    kwargs: dict[str, Any],
    pos: int,
    key: str,
) -> Optional[Any]:
    if len(args) > pos:
        return args[pos]
    value = kwargs.get(key)
    return value if value is not None else None


def __resolve_event(
    args: Tuple[Any, ...],
    kwargs: dict[str, Any],
) -> Optional[Mapping[str, Any]]:
    result = __get_arg_or_kwarg(args, kwargs, 0, "event")
    return cast(Optional[Mapping[str, Any]], result)


def __resolve_context(
    args: Tuple[Any, ...],
    kwargs: dict[str, Any],
) -> Optional[Context]:
    result = __get_arg_or_kwarg(args, kwargs, 1, "context")
    return cast(Optional[Context], result)


def __is_dry_run_enabled(value: Any) -> bool:
    """Check if dry run mode is enabled.
    
    :param value: Value to check (string or boolean)
    :type value: Any
    :return: True if dry run is enabled
    :rtype: bool
    """
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.lower() in ('true', 't', 'y', 'yes')
    return False


def tracer_decorator(
    is_scheduled: bool = False,
    is_job: bool = False,
    component: Optional[str] = None,
) -> Callable[[Callable[P, R]], Callable[P, R]]:
    """Decorator for tracing function execution with job and segment tracking.
    
    :param is_scheduled: Whether the function is scheduled
    :type is_scheduled: bool
    :param is_job: Whether to enable job tracing
    :type is_job: bool
    :param component: Component name for job tracing
    :type component: str, optional
    
    .. important::
        Functions decorated with this decorator MUST accept 'event' and 'context'
        as keyword-only arguments to avoid bugs.
    
    .. warning::
        Potential issues with regular parameters:
        
        - Decorator expects args[0] = event, args[1] = context
        - If function signature has wrong parameter order (e.g. context, event),
          decorator will receive them in wrong order causing silent bugs
        - This happens because regular parameters are passed via *args in definition order
        
        Do NOT use ``def handler(event, context):`` or ``def handler(context, event):``
        Without keyword-only marker '*', wrong parameter order in function definition
        will cause decorator to misinterpret which argument is event and which is context!
    
    **Recommended function signature** (use '*' to enforce keyword-only)::
    
        @tracer_decorator(is_job=True, component='my_component')
        def handler(*, event, context):  # <- '*' forces keyword-only args
            ...
    
    **Usage example**::
    
        handler(event={'data': 'value'}, context=mock_context)
    """
    def real_wrapper(func: Callable[P, R]) -> Callable[P, R]:
        @wraps(func)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:

            from modular_sdk.services.environment_service import (
                EnvironmentService,
            )
            from modular_sdk.utils.job_tracer.generic import (
                ModularJobTracer,
            )
            from modular_sdk.services.sqs_service import SQSService
            
            environment_service: EnvironmentService = Modular().environment_service()
            sqs_service: SQSService = Modular().sqs_service()

            job_tracer: Optional[ModularJobTracer] = None
            request_id: Optional[str] = None
            event: Mapping[str, Any] = {}
            
            if is_job:
                operation_mode_service = ModularOperationModeManagerService()
                job_tracer = ModularJobTracer(
                    operation_mode_service=operation_mode_service,
                    environment_service=environment_service,
                    component=component
                )

                event = __resolve_event(args, kwargs) or {}
                context = __resolve_context(args, kwargs)
                request_id = (
                    event.get('request_id')
                    or (context.aws_request_id if context else None)
                    or str(uuid4())
                )

            runtime_tracer: SegmentTracer | ScheduledSegmentTracer
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
                if __is_dry_run_enabled(dry_run):
                    meta = {'message': 'Dry run mode enabled.'}
                job_tracer.start(job_id=request_id, meta=meta)
            try:
                response = func(*args, **kwargs)
            except Exception as e:
                if is_job:
                    job_tracer.fail(job_id=request_id, error=e)
                segment.error()
                raise
            else:
                if is_job:
                    job_tracer.succeed(job_id=request_id, meta=response)
                segment.stop()
                return response

        return wrapper

    return real_wrapper
