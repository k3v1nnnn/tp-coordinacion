from common import middleware, message_protocol
import logging
import threading

class FruitConsumer:
    def __init__(self, id, host, prefix, manager, producer):
        self.lock = threading.Lock()
        self.manager = manager
        self.producer = producer
        self.input = middleware.MessageMiddlewareExchangeRabbitMQ(host, prefix, [f"{prefix}_{id}"])

    def _fruit_message(self, client_id, fruit, amount):
        with self.lock:
            self.manager.add(client_id, fruit, amount)
    
    def _end_message(self, client_id):
        with self.lock:
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

    def run(self):
        input_thread = threading.Thread(
            target=self.input.start_consuming, 
            args=(self.process_message,))
        input_thread.start()
        input_thread.join()