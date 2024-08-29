import pika

from modular_sdk.commons import ModularException
from modular_sdk.commons.log_helper import get_logger

RABBIT_DEFAULT_RESPONSE_TIMEOUT = 30
_LOG = get_logger('modular_sdk-rabbit_connection')


class RabbitMqConnection:
    def __init__(self, connection_url, timeout):
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
        if self.conn.is_open:
            self.conn.close()

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
    def __basic_publish(channel, **kwargs):
        try:
            channel.basic_publish(**kwargs)
            return True
        except (pika.exceptions.NackError, pika.exceptions.UnroutableError):
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

    def consume_sync(self, queue, correlation_id):

        def _consumer_callback(ch, method, props, body):
            self.responses[props.correlation_id] = body
            ch.basic_ack(delivery_tag=method.delivery_tag)
            ch.stop_consuming(props.correlation_id)

        def _close_on_timeout():
            _LOG.warn('Timeout exceeded. Close connection')
            self.conn.close()

        channel = self._open_channel()
        if channel.basic_consume(queue=queue,
                                 on_message_callback=_consumer_callback,
                                 consumer_tag=correlation_id):
            _LOG.debug('Waiting for message. Queue: {0}, Correlation id: {1}'
                       .format(queue, correlation_id))
        else:
            _LOG.error('Failed to consume. Queue: {0}'.format(queue))
            return None

        self.conn.add_timeout(self.timeout, _close_on_timeout)

        # blocking method
        channel.start_consuming()
        self._close()

        if correlation_id in list(self.responses.keys()):
            response = self.responses.pop(correlation_id)
            _LOG.debug('Response received')
            return response
        else:
            _LOG.error('Response was not received. Timeout: {0} seconds. '
                       .format(self.timeout))
            return None

    def check_queue_exists(self, queue_name):
        channel = self._open_channel()
        try:
            channel.queue_declare(queue=queue_name, durable=True, passive=True)
        except pika.exceptions.ChannelClosedByBroker as e:
            if e.reply_code == 404:
                return False
        self._close()
        return True

    def declare_queue(self, queue_name):
        channel = self._open_channel()
        declare_resp = channel.queue_declare(queue=queue_name, durable=True)
        _LOG.info('Queue declaration response: {0}'.format(declare_resp))
