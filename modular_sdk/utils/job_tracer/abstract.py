from abc import abstractmethod, ABC


class AbstractJobTracer(ABC):
    @abstractmethod
    def start(self, job_id):
        pass

    @abstractmethod
    def is_permitted_to_start(self):
        pass

    @abstractmethod
    def fail(self, request_id, error: Exception):
        pass

    @abstractmethod
    def succeed(self, request_id, meta):
        pass
