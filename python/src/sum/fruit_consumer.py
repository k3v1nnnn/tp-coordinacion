from common import middleware, message_protocol
import logging
import threading

class FruitConsumer:
    def __init__(self, consumer_id, host, name_queue, prefix, manager, notifier):
        self.lock = threading.Lock()
        self.manager = manager
        self.notifier = notifier
        self.input = middleware.MessageMiddlewareQueueRabbitMQ(host, name_queue)
        self.own_input = middleware.MessageMiddlewareExchangeRabbitMQ(host, prefix, [f"{prefix}_{consumer_id}"])

    def _fruit_message(self, client_id, fruit, amount):
        with self.lock:
            self.manager.add(client_id, fruit, amount)
    
    def _end_message(self, client_id, expand=True):
        with self.lock:
            fruits = self.manager.get(client_id)
            self.notifier.notify(client_id, fruits, expand)

    def process_message(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        if len(fields) == 3:
            self._fruit_message(*fields)
        elif len(fields) == 2:
            self._end_message(*fields)
        elif len(fields) == 1:
            self._end_message(*fields)
        else:
            return nack()
        ack()

    def run(self):
        input_thread = threading.Thread(
            target=self.input.start_consuming, 
            args=(self.process_message,))
        own_input_thread = threading.Thread(
            target=self.own_input.start_consuming, 
            args=(self.process_message,))
        input_thread.start()
        own_input_thread.start()
        input_thread.join()
        own_input_thread.join()