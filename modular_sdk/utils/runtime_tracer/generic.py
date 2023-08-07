import json
import re
from datetime import datetime
from modular_sdk.services.environment_service import EnvironmentService
from modular_sdk.services.events_service import EventsService
from modular_sdk.services.lambda_service import LambdaService
from modular_sdk.services.sqs_service import SQSService
from modular_sdk.utils.runtime_tracer.abstract import AbstractSegmentTracer, \
    AbstractSegment


class Segment(AbstractSegment):
    def __init__(self, name: str, tracer: AbstractSegmentTracer):
        self.name = name
        self.started_at = datetime.utcnow()
        self.stopped_at = None
        self.execution_time = None
        self.tracer = tracer
        self.is_error = True

    def _calculate_execution_time(self):
        time_delta = self.stopped_at - self.started_at
        return time_delta.seconds

    def stop(self):
        self.stopped_at = datetime.utcnow()
        self.execution_time = self._calculate_execution_time()
        self.is_error = False
        self.tracer.stop_segment(self)

    def error(self):
        self.stopped_at = datetime.utcnow()
        self.execution_time = self._calculate_execution_time()
        self.tracer.stop_segment(self)


class SegmentTracer(AbstractSegmentTracer):
    def __init__(self, sqs_service: SQSService,
                 environment_service: EnvironmentService):
        self.sqs_service = sqs_service
        self.environment_service = environment_service
        self.unprocessed_traces = {}
        self.processed_traces = []

    def start(self):
        import uuid
        name = uuid.uuid4().hex
        segment = Segment(
            name=name,
            tracer=self
        )
        self.unprocessed_traces[name] = segment
        return segment

    def build_sqs_message(self, segment: AbstractSegment):
        message = {
            'application_name': self.environment_service.application(),
            'component_name': self.environment_service.component(),
            'isError': segment.is_error,
            'execution_time': segment.execution_time
        }
        return message

    def save(self, processed_traces):
        for segment in processed_traces:
            message = self.build_sqs_message(segment=segment)
            self.sqs_service.send_message(message=message)

    def stop_segment(self, segment: AbstractSegment):
        processed_segment = self.unprocessed_traces.pop(segment.name)
        self.processed_traces.append(processed_segment)
        if not self.unprocessed_traces:
            self.save(processed_traces=self.processed_traces)


class ScheduledSegmentTracer(SegmentTracer):
    def __init__(self, sqs_service: SQSService, lambda_service: LambdaService,
                 events_service: EventsService,
                 environment_service: EnvironmentService):
        super(ScheduledSegmentTracer, self).__init__(
            sqs_service=sqs_service,
            environment_service=environment_service
        )
        self.lambda_service = lambda_service
        self.events_service = events_service

    def _get_event_rule(self):
        lambda_name = self.environment_service.component()
        policy_meta = self.lambda_service.get_policy(name=lambda_name)
        policy = json.loads(policy_meta['Policy'])
        rule = None
        for statement in policy.get('Statement', {}):
            rule_arn = statement.get('Condition', {}).get('ArnLike', {}).get(
                'AWS:SourceArn', ''
            )
            if re.match(r"(?:arn:aws:events:[a-z0-9-]+:\d{12}:rule/)(.+)", rule_arn) \
                    and statement['Action'] == 'lambda:InvokeFunction':
                rule = rule_arn.split('/')[-1]
        if not rule:
            return

        rule_meta = self.events_service.describe_rule(name=rule)
        rule_expressions = rule_meta['ScheduleExpression']
        return rule_expressions

    def build_sqs_message(self, segment):
        message = super(ScheduledSegmentTracer, self).build_sqs_message(
            segment=segment
        )
        message['cron'] = self._get_event_rule()
        return message
