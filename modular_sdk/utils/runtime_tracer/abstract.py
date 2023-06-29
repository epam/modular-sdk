from abc import abstractmethod, ABC
from datetime import datetime
from modular_sdk.services.environment_service import EnvironmentService
from modular_sdk.services.sqs_service import SQSService


class AbstractSegment(ABC):
    @abstractmethod
    def __init__(self, name: str, tracer):
        self.name = name
        self.started_at = datetime.utcnow()
        self.stopped_at = None
        self.execution_time = None
        self.tracer = tracer
        self.is_error = True

    @abstractmethod
    def stop(self):
        pass

    @abstractmethod
    def error(self):
        pass


class AbstractSegmentTracer(ABC):
    @abstractmethod
    def __init__(self, sqs_service: SQSService,
                 environment_service: EnvironmentService):
        self.sqs_service = sqs_service
        self.environment_service = environment_service

    @abstractmethod
    def start(self):
        pass

    @abstractmethod
    def save(self, processed_traces):
        pass

    @abstractmethod
    def stop_segment(self, segment: AbstractSegment):
        pass
