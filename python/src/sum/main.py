import os
import logging
import threading

from common import middleware, message_protocol, fruit_item
from fruit_counter import FruitCounter
from fruit_notifier import FruitNotifier
from fruit_consumer import FruitConsumer

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
SUM_CONTROL_EXCHANGE = "SUM_CONTROL_EXCHANGE"
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]

class SumFilter:
    def __init__(self):
        self.counter = FruitCounter()
        self.notifier = FruitNotifier(ID, MOM_HOST, AGGREGATION_PREFIX, AGGREGATION_AMOUNT, SUM_PREFIX, SUM_AMOUNT)
        self.consumer = FruitConsumer(ID, MOM_HOST, INPUT_QUEUE, SUM_PREFIX, self.counter, self.notifier)

    def start(self):
        self.consumer.run()


def main():
    logging.basicConfig(level=logging.INFO)
    sum_filter = SumFilter()
    sum_filter.start()
    return 0


if __name__ == "__main__":
    main()
