from common import middleware, message_protocol

class FruitNotifier:
    def __init__(self, id, host, prefix, amount, own_prefix, own_amount):
        self.outputs = []
        self.own_outputs = []
        self.amount = amount
        self.own_amount = own_amount
        for i in range(amount):
            self.outputs.append(middleware.MessageMiddlewareExchangeRabbitMQ(host, prefix, [f"{prefix}_{i}"]))
        for i in range(own_amount):
            if i != id:
                self.own_outputs.append(middleware.MessageMiddlewareExchangeRabbitMQ(host, own_prefix, [f"{own_prefix}_{i}"]))

    def _assign_output(self, fruit):
        return hash(fruit.lower()) % self.amount

    def notify(self, client_id, fruits, expand=True):
        for fruit in fruits:
            id_output = self._assign_output(fruit[1])
            self.outputs[id_output].send(message_protocol.internal.serialize(fruit))
        
        for output in self.outputs:
            output.send(message_protocol.internal.serialize([client_id]))

        if expand :
            for own_output in self.own_outputs:
                own_output.send(message_protocol.internal.serialize([client_id, False]))

