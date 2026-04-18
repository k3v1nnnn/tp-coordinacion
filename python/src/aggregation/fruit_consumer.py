from common import middleware, message_protocol
import signal

class FruitConsumer:
    def __init__(self, consumer_id, host, prefix, manager, producer):
        self.manager = manager
        self.producer = producer
        self.input = middleware.MessageMiddlewareExchangeRabbitMQ(host, prefix, [f"{prefix}_{consumer_id}"])

    def _fruit_message(self, client_id, fruit, amount):
        self.manager.add(client_id, fruit, amount)

    def _end_message(self, client_id):
        if self.producer.can_produce(client_id):
            fruits = self.manager.get(client_id)
            self.producer.produce(client_id, fruits)

    def process_message(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        if len(fields) == 3:
            self._fruit_message(*fields)
        elif len(fields) == 1:
            self._end_message(*fields)
        else:
            return nack()
        ack()

    def _stop(self, *_):
        self.input.stop_consuming()

    def run(self):
        signal.signal(signal.SIGTERM, self._stop)
        self.input.start_consuming(self.process_message)
        self.input.close()