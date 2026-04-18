import os
import logging
import bisect

from common import middleware, message_protocol, fruit_item
from fruit_top import FruitTop
from fruit_consumer import FruitConsumer
from fruit_producer import FruitProducer

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])


class AggregationFilter:

    def __init__(self):
        self.top = FruitTop()
        self.producer = FruitProducer(MOM_HOST, OUTPUT_QUEUE, SUM_AMOUNT)
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
