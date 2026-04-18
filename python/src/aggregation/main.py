import os
import logging

from common import fruit_producer, fruit_top
from fruit_consumer import FruitConsumer

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]


class AggregationFilter:

    def __init__(self):
        self.top = fruit_top.FruitTop()
        self.producer = fruit_producer.FruitProducer(MOM_HOST, OUTPUT_QUEUE, SUM_AMOUNT)
        self.consumer = FruitConsumer(ID, MOM_HOST, AGGREGATION_PREFIX, self.top, self.producer)

    def start(self):
        self.consumer.run()


def main():
    logging.basicConfig(level=logging.INFO)
    aggregation_filter = AggregationFilter()
    aggregation_filter.start()
    return 0


if __name__ == "__main__":
    main()
