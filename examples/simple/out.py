"""
data sink: outputs list items
"""

import sys

from pipeline.worker import ListConsumerWorker, run

class Out(ListConsumerWorker):
    """
    takes lists of ints and prints them.
    """
    INPUT_BATCH_MSGS = 10       # process 10 messages at a time

    def __init__(self, process_name: str, descr: str):
        super().__init__(process_name, descr)
        self.items = []

    def process_item(self, item):
        self.items.append(item)

    def end_of_batch(self, chan):
        print("out:", self.items)
        self.items = []
        sys.stdout.flush()
        return None

run(Out, "simple-out", "output worker for simple pipeline")
