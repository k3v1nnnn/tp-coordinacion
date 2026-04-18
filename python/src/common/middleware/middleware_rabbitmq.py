import pika
from .middleware import (
    MessageMiddlewareQueue, MessageMiddlewareExchange,
    MessageMiddlewareMessageError, MessageMiddlewareDisconnectedError, MessageMiddlewareCloseError
)

class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):

    QUEUE_TYPE = 'quorum'
    PREFETCH_COUNT = 1
    DEFAULT_EXCHANGE = ''

    def __init__(self, host, queue_name):
        params = pika.ConnectionParameters(host=host)
        self.connection = pika.BlockingConnection(params)
        self.queue = queue_name
        self.channel = self.connection.channel()
        self.channel.queue_declare(
            queue=queue_name,
            durable=True,
            arguments={'x-queue-type': self.QUEUE_TYPE}
        )

    def start_consuming(self, on_message_callback):
        if self.connection.is_open:
            try:
                def _wrapper(channel, method, _, message):
                    ack = lambda: channel.basic_ack(delivery_tag=method.delivery_tag)
                    nack = lambda: channel.basic_nack(delivery_tag=method.delivery_tag)
                    on_message_callback(message, ack, nack)

                self.channel.basic_qos(prefetch_count=self.PREFETCH_COUNT)
                self.channel.basic_consume(queue=self.queue, on_message_callback=_wrapper)
                self.channel.start_consuming()
            except Exception as e:
                raise MessageMiddlewareMessageError() from e
        else:
            raise MessageMiddlewareDisconnectedError()

    def stop_consuming(self):
        if self.connection.is_open:
            self.channel.stop_consuming()
        else:
            raise MessageMiddlewareDisconnectedError()

    def send(self, message):
        if self.connection.is_open:
            try:
                self.channel.basic_publish(
                    exchange=self.DEFAULT_EXCHANGE,
                    routing_key=self.queue,
                    body=message,
                    properties=pika.BasicProperties(
                        delivery_mode=pika.DeliveryMode.Persistent
                    )
                )
            except Exception as e:
                raise MessageMiddlewareMessageError() from e
        else:
            raise MessageMiddlewareDisconnectedError()

    def close(self):
        if self.connection.is_open:
            try:
                self.connection.close()
            except Exception as e:
                raise MessageMiddlewareCloseError() from e


class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):

    EXCHANGE_TYPE = 'direct'

    def __init__(self, host, exchange_name, routing_keys):
        params = pika.ConnectionParameters(host=host)
        self.exchange_name = exchange_name
        self.routing_keys = routing_keys
        self.connection = pika.BlockingConnection(params)
        self.channel = self.connection.channel()
        self.channel.exchange_declare(exchange=exchange_name, exchange_type=self.EXCHANGE_TYPE)
        queue_declare = self.channel.queue_declare(queue='', exclusive=True)
        self.queue = queue_declare.method.queue
        for routing_key in routing_keys:
            self.channel.queue_bind(exchange=self.exchange_name, queue=self.queue, routing_key=routing_key)

    def start_consuming(self, on_message_callback):
        if self.connection.is_open:
            try:
                def _wrapper(channel, method, _, message):
                    ack = lambda: channel.basic_ack(delivery_tag=method.delivery_tag)
                    nack = lambda: channel.basic_nack(delivery_tag=method.delivery_tag)
                    on_message_callback(message, ack, nack)

                self.channel.basic_consume(queue=self.queue, on_message_callback=_wrapper)
                self.channel.start_consuming()
            except Exception as e:
                raise MessageMiddlewareMessageError() from e
        else:
            raise MessageMiddlewareDisconnectedError()

    def stop_consuming(self):
        if self.connection.is_open:
            self.channel.stop_consuming()
        else:
            raise MessageMiddlewareDisconnectedError()

    def send(self, message):
        if self.connection.is_open:
            try:
                for routing_key in self.routing_keys:
                    self.channel.basic_publish(exchange=self.exchange_name, routing_key=routing_key, body=message)
            except Exception as e:
                raise MessageMiddlewareMessageError() from e
        else:
            raise MessageMiddlewareDisconnectedError()

    def close(self):
        if self.connection.is_open:
            try:
                self.connection.close()
            except Exception as e:
                raise MessageMiddlewareCloseError() from e
