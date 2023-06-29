import pika

from modular_sdk.commons import ModularException
from modular_sdk.commons.log_helper import get_logger

RABBIT_DEFAULT_RESPONSE_TIMEOUT = 30
_LOG = get_logger('modular_sdk-rabbit_connection')


class RabbitMqConnection:
    def __init__(self, connection_url,
                 timeout):
        self.connection_url = connection_url
        self.timeout = timeout or RABBIT_DEFAULT_RESPONSE_TIMEOUT
        self.responses = {}

    def _open_channel(self):
        try:
            parameters = pika.URLParameters(self.connection_url)
            self.conn = pika.BlockingConnection(parameters)
            return self.conn.channel()
        except pika.exceptions.AMQPConnectionError as e:
            _LOG.error('Connection to RabbitMQ refused. Bad credentials.')
            raise ModularException(
                code=502,
                content='Connection to RabbitMQ refused. Bad credentials.'
            )

    def _close(self):
        if self.conn.is_open:
            self.conn.close()

    def publish(self, message, routing_key, exchange='', headers=None,
                content_type=None):
        channel = self._open_channel()
        channel.confirm_delivery()
        response = channel.basic_publish(
            exchange=exchange,
            routing_key=routing_key,
            properties=pika.BasicProperties(headers=headers,
                                            content_type=content_type),
            body=message,
            mandatory=True)
        self._close()
        if not response:
            message = ('Message was returned. Check RabbitMQ '
                       'configuration: maybe target queue does not exists.')
            _LOG.error(message)
            raise ModularException(code=504, content=message)
        _LOG.info('Message pushed')

    def publish_sync(self, message, routing_key, correlation_id,
                     callback_queue, exchange='', headers=None,
                     content_type=None):

        channel = self._open_channel()
        channel.confirm_delivery()
        response = channel.basic_publish(
            exchange=exchange,
            routing_key=routing_key,
            properties=pika.BasicProperties(headers=headers,
                                            reply_to=callback_queue,
                                            correlation_id=correlation_id,
                                            content_type=content_type),
            body=message)
        if not response:
            message = ('Message was returned. Check RabbitMQ '
                       'configuration: maybe target queue does not exists.')
            _LOG.error(message)
            raise ModularException(code=504, content=message)
        _LOG.info('Message pushed')

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
            _LOG.error('Response was not received. '
                       'Timeout: {0} seconds. '
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
