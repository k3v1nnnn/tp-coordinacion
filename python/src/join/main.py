import os
import logging

from common import middleware, message_protocol, fruit_item

MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])


class JoinFilter:

    def __init__(self):
        self.top = FruitTop(TOP_SIZE)
        self.producer = FruitProducer(MOM_HOST, OUTPUT_QUEUE, AGGREGATION_AMOUNT)
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )

    def process_messsage(self, message, ack, nack):
        logging.info("Received top")
        client_top = message_protocol.internal.deserialize(message)
        client_id = client_top[0]
        fruits_top = client_top[1]
        for fruit in fruits_top:
            self.top.add(client_id, fruit[0], fruit[1])
        if self.producer.can_produce(client_id):
            fruits = self.top.get(client_id)
            self.producer.produce(client_id, fruits)
        ack()

    def start(self):
        self.input_queue.start_consuming(self.process_messsage)


def main():
    logging.basicConfig(level=logging.INFO)
    join_filter = JoinFilter()
    join_filter.start()

    return 0


if __name__ == "__main__":
    main()
