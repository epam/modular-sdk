import pika
import pika.exceptions

from modular_sdk.commons import ModularException
from modular_sdk.commons.log_helper import get_logger

RABBIT_DEFAULT_RESPONSE_TIMEOUT = 30
_LOG = get_logger('modular_sdk-rabbit_connection')


class RabbitMqConnection:
    def __init__(self, connection_url: str, timeout: int):
        self.connection_url = connection_url
        self.timeout = timeout or RABBIT_DEFAULT_RESPONSE_TIMEOUT
        self.responses = {}

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

    def _close(self):
        try:
            if self.conn.is_open:
                self.conn.close()
        except Exception as e:
            _LOG.error(f"Failed to close RabbitMQ connection: {e}")

    def publish(
            self,
            message: str,
            routing_key: str,
            exchange: str = '',
            headers: dict = None,
            content_type: str = None,
    ) -> None:
        _LOG.debug(f'Request queue: {routing_key}')
        channel = self._open_channel()
        try:
            channel.confirm_delivery()
            if not self.__basic_publish(
                    channel=channel,
                    exchange=exchange,
                    routing_key=routing_key,
                    properties=pika.BasicProperties(
                        headers=headers, content_type=content_type,
                    ),
                    body=message,
                    mandatory=True,
            ):
                _LOG.error(
                    f'Message was not sent: routing_key={routing_key}, '
                    f'exchange={exchange}, content_type={content_type}'
                )
                raise ModularException(
                    code=504,
                    content='Message was not sent. Check RabbitMQ configuration'
                )
            _LOG.info('Message pushed')
        finally:
            self._close()

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

    def publish_sync(
            self,
            message: str,
            routing_key: str,
            correlation_id: str,
            callback_queue: str,
            exchange: str = '',
            headers: dict = None,
            content_type: str = None,
    ) -> None:
        _LOG.debug(
            f'Request queue: {routing_key}; Response queue: {callback_queue}'
        )
        channel = self._open_channel()
        try:
            channel.confirm_delivery()
            properties = pika.BasicProperties(
                headers=headers,
                reply_to=callback_queue,
                correlation_id=correlation_id,
                content_type=content_type,
            )
            if not self.__basic_publish(
                    channel=channel,
                    exchange=exchange,
                    routing_key=routing_key,
                    properties=properties,
                    body=message,
            ):
                error_msg = (
                    f"Message was not sent: routing_key={routing_key}, "
                    f"correlation_id={correlation_id}, "
                    f"callback_queue={callback_queue}, "
                    f"exchange={exchange}, content_type={content_type}"
                )
                _LOG.error(error_msg)
                raise ModularException(
                    code=504,
                    content='Message was not sent. Check RabbitMQ configuration'
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
            _LOG.error(f"Failed to consume message from queue '{queue}'")
            return None

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
        _LOG.info('Queue declaration response: {0}'.format(declare_resp))
