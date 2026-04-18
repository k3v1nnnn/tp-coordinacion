from common import middleware, message_protocol

class FruitProducer:
    def __init__(self, host, name_queue, amount):
        self.output = middleware.MessageMiddlewareQueueRabbitMQ(host, name_queue)
        self.amount = amount
        self.client_counter = {}

    def can_produce(self, client_id):
        client_count = self.client_counter.get(client_id, 0)
        self.client_counter[client_id] = client_count = client_count + 1
        return client_count == self.amount

    def produce(self, client_id, fruits):
        self.output.send(message_protocol.internal.serialize([client_id, fruits]))
