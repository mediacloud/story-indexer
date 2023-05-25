"""
data sink: outputs list items
"""

import sys
from pathlib import Path
from pipeline.worker import ListConsumerWorker, run

class Out(ListConsumerWorker):
    """
    takes lists of ints and prints them.
    """
    INPUT_BATCH_MSGS = 10       # process 10 messages at a time

    def __init__(self, process_name: str, descr: str):
        super().__init__(process_name, descr)
        self.path = "data/out.txt"
        if not Path("data/").exists():
            Path("data/").mkdir()
        Path(self.path).touch()
        self.items = []

    def process_item(self, item):
        self.items.append(item)
        print(item)
        Path(self.path).write_text(item+"\n")

    def end_of_batch(self, chan):
        print("out:", self.items)
        self.items = []
        sys.stdout.flush()
        return None

run(Out, "fetcher-out", "output worker for simple pipeline")