import pika
from pika import exceptions

from modular_sdk.commons import ModularException
from modular_sdk.commons.constants import PLAIN_CONTENT_TYPE
from modular_sdk.commons.error_helper import (
    CONFIGURATION_ISSUES_ERROR_MESSAGE, TIMEOUT_ERROR_MESSAGE,
)
from modular_sdk.commons.log_helper import get_logger
from modular_sdk.strategies.abstract_strategy import AbstractStrategy

RABBIT_DEFAULT_RESPONSE_TIMEOUT = 30

_LOG = get_logger(__name__)


class RabbitMQStrategy(AbstractStrategy):
    def __init__(
            self,
            sdk_access_key: str,
            sdk_secret_key: str,
            maestro_user: str,
            connection_url: str,
            request_queue: str,
            response_queue: str,
            rabbit_exchange: str,
            timeout: int = RABBIT_DEFAULT_RESPONSE_TIMEOUT,
    ):
        super().__init__(
            access_key=sdk_access_key,
            secret_key=sdk_secret_key,
            user=maestro_user,
        )
        self.connection_url = connection_url
        self.request_queue = request_queue
        self.response_queue = response_queue
        self.rabbit_exchange = rabbit_exchange or ''
        self.timeout = timeout
        self.responses = {}

    def _close(self):
        try:
            if self.conn.is_open:
                self.conn.close()
        except Exception as e:
            _LOG.error(f"Failed to close RabbitMQ connection: {e}")

    def _open_channel(self):
        try:
            parameters = pika.URLParameters(self.connection_url)
            self.conn = pika.BlockingConnection(parameters)
            return self.conn.channel()
        except pika.exceptions.AMQPConnectionError as e:
            error_msg = str(e) or "Bad credentials"
            _LOG.error(f'Connection to RabbitMQ refused: {error_msg}')
            raise ModularException(
                code=502, content=f'Connection to RabbitMQ refused: {error_msg}'
            )

    @staticmethod
    def __basic_publish(
            channel: pika.adapters.blocking_connection.BlockingChannel,
            **kwargs,
    ) -> bool:
        try:
            channel.basic_publish(**kwargs)
            return True
        except (pika.exceptions.NackError, pika.exceptions.UnroutableError):
            _LOG.exception('Pika exception occurred')
            return False

    def publish(
            self,
            request_id: str,
            message: bytes,
            headers: dict,
    ) -> bytes:
        self.publish_sync(
            routing_key=self.request_queue,
            correlation_id=request_id,
            message=message,
            headers=headers,
            content_type=PLAIN_CONTENT_TYPE,
        )
        try:
            response_item = self.consume_sync(
                queue=self.response_queue, correlation_id=request_id,
            )
        except exceptions.ConnectionWrongStateError as e:
            raise ModularException(code=502, content=str(e))
        if not response_item:
            raise ModularException(
                code=502,
                content=f"Response wasn't received. Timeout: {self.timeout} seconds"
            )
        return response_item

    # def publish_async(self, *args, **kwargs):
    #     request_id, message, headers = self._pre_process_request(*args, **kwargs)
    #     return self.publish(request_id=request_id, message=message, headers=headers)

    def publish_sync(
            self,
            message: bytes,
            routing_key: str,
            correlation_id: str,
            headers: dict = None,
            content_type: str = None,
    ) -> None:
        _LOG.debug(
            f'Request queue: {self.request_queue}; '
            f'Response queue: {self.response_queue}'
        )
        channel = self._open_channel()
        try:
            channel.confirm_delivery()
            properties = pika.BasicProperties(
                headers=headers,
                reply_to=self.response_queue,
                correlation_id=correlation_id,
                content_type=content_type,
            )
            if not self.__basic_publish(
                    channel=channel,
                    exchange=self.rabbit_exchange,
                    routing_key=routing_key,
                    properties=properties,
                    body=message,
            ):
                _LOG.error(
                    'Message event was returned. Check RabbitMQ configuration: '
                    'maybe target queue does not exists.'
                )
                raise ModularException(
                    code=502, content=CONFIGURATION_ISSUES_ERROR_MESSAGE,
                )
            _LOG.info('Message pushed')
        finally:
            self._close()

    def consume_sync(self, queue: str, correlation_id: str) -> bytes | None:

        def _consumer_callback(
                ch: pika.adapters.blocking_connection.BlockingChannel,
                method: pika.spec.Basic.Deliver,
                props: pika.spec.BasicProperties,
                body: bytes,
        ) -> None:
            if props.correlation_id == correlation_id:
                _LOG.debug(
                    f'Message retrieved successfully with ID: '
                    f'{props.correlation_id}'
                )
                self.responses[props.correlation_id] = body
                ch.basic_ack(delivery_tag=method.delivery_tag)
                ch.stop_consuming()
            else:
                _LOG.warning(
                    f'Received message with mismatched Correlation ID:'
                    f'{props.correlation_id} (expected: {correlation_id})'
                )
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

        def _close_on_timeout():
            _LOG.warning('Timeout exceeded. Close connection')
            self._close()

        channel = self._open_channel()
        if channel.basic_consume(
                queue=queue,
                on_message_callback=_consumer_callback,
                consumer_tag=correlation_id,
        ):
            _LOG.debug(
                f'Waiting for message. Queue: {queue}, '
                f'Correlation id: {correlation_id}'
            )
        else:
            _LOG.error(f"Failed to consume. Queue: '{queue}'")
            raise ModularException(
                code=502, content=TIMEOUT_ERROR_MESSAGE,
            )

        self.conn.call_later(self.timeout, _close_on_timeout)
        # blocking method
        channel.start_consuming()
        self._close()

        response = self.responses.pop(correlation_id, None)
        if response:
            _LOG.debug('Response successfully received and processed')
            return response
        _LOG.error(f"Response wasn't received. Timeout: {self.timeout} seconds")
        return None

    def check_queue_exists(self, queue_name: str) -> bool:
        channel = self._open_channel()
        try:
            channel.queue_declare(queue=queue_name, durable=True, passive=True)
        except pika.exceptions.ChannelClosedByBroker as e:
            if e.reply_code == 404:
                return False
        self._close()
        return True

    def declare_queue(self, queue_name: str) -> None:
        channel = self._open_channel()
        declare_resp = channel.queue_declare(queue=queue_name, durable=True)
        _LOG.info(f'Queue declaration response: {declare_resp}')
