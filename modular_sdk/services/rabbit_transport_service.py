from abc import abstractmethod
import uuid
from modular_sdk.commons import ModularException
from modular_sdk.commons.log_helper import get_logger
from pika import exceptions

_LOG = get_logger('RemoteExecutionService')

PLAIN_CONTENT_TYPE = 'text/plain'
SUCCESS_STATUS = 'SUCCESS'
ERROR_STATUS = 'FAILED'
RESULTS = 'results'
DATA = 'data'


class RabbitConfig:
    def __init__(self, request_queue, response_queue, rabbit_exchange):
        self.request_queue = request_queue
        self.response_queue = response_queue
        self.rabbit_exchange = rabbit_exchange


class RabbitMQTransport:
    def __init__(self, rabbit_connection, config):
        self.rabbit = rabbit_connection
        self.request_queue = config.request_queue
        self.response_queue = config.response_queue
        self.exchange = config.rabbit_exchange

    @abstractmethod
    def pre_process_request(self, *args, **kwargs):
        # signing, encypt
        pass

    @abstractmethod
    def post_process_request(self, *args, **kwargs):
        # sign check, decrypt
        pass

    def __resolve_rabbit_options(self, exchange, request_queue, response_queue):
        exchange = exchange or self.exchange
        if exchange:
            routing_key = ''
        else:
            routing_key = request_queue or self.request_queue
            exchange = ''

        response_queue = response_queue if response_queue else self.response_queue
        return routing_key, exchange, response_queue

    def send_sync(self, *args, **kwargs):
        message, headers = self.pre_process_request(*args, **kwargs)
        rabbit_config = kwargs.get('config')
        request_queue, exchange, response_queue = \
            self.__resolve_rabbit_options(
                exchange=rabbit_config.rabbit_exchange if rabbit_config else None,
                request_queue=rabbit_config.request_queue if rabbit_config else None,
                response_queue=rabbit_config.response_queue if rabbit_config else None
            )

        request_id = uuid.uuid4().hex
        self.rabbit.publish_sync(routing_key=request_queue,
                                 exchange=exchange,
                                 callback_queue=response_queue,
                                 correlation_id=request_id,
                                 message=message,
                                 headers=headers,
                                 content_type=PLAIN_CONTENT_TYPE)
        try:
            response_item = self.rabbit.consume_sync(queue=response_queue,
                                                     correlation_id=request_id)
        except exceptions.ConnectionWrongStateError as e:
            raise ModularException(code=502, content=str(e))

        if not response_item:
            raise ModularException(
                code=502,
                content=f'Response was not received. '
                        f'Timeout: {self.rabbit.timeout} seconds.'
            )
        return self.post_process_request(response=response_item)

    def send_async(self, *args, **kwargs):
        message, headers = self.pre_process_request(*args, **kwargs)
        rabbit_config = kwargs.get('config')
        request_queue, exchange, response_queue = \
            self.__resolve_rabbit_options(
                exchange=rabbit_config.exchange if rabbit_config else None,
                request_queue=rabbit_config.request_queue if rabbit_config else None,
                response_queue=rabbit_config.response_queue if rabbit_config else None
            )

        return self.rabbit.publish(
            routing_key=request_queue,
            exchange=exchange,
            message=message,
            headers=headers,
            content_type=PLAIN_CONTENT_TYPE)

