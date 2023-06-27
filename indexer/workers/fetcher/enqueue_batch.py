import argparse
import csv
import logging
import sys
from typing import Any, Dict, List, Optional, Tuple, TypedDict

from scrapy.crawler import CrawlerProcess

from indexer.path import DATAPATH_BY_DATE
from indexer.story import BaseStory, DiskStory
from indexer.worker import QApp

"""
App interface to launching a scrapy crawler on a set of batched stories
"""

logger = logging.getLogger(__name__)


class BatchQueuer(QApp):
    date: str
    batch_index: int

    def define_options(self, ap: argparse.ArgumentParser) -> None:
        super().define_options(ap)

        # fetch_date
        ap.add_argument(
            "--fetch-date",
            dest="fetch_date",
            help="Date (in YYYY-MM-DD) to fetch",
        )

        # sample_size
        ap.add_argument(
            "--batch-index",
            dest="batch_index",
            type=int,
            default=None,
            help="The index of the batch to work on fetching",
        )

    def process_args(self) -> None:
        super().process_args()

        assert self.args
        logger.info(self.args)

        fetch_date = self.args.fetch_date
        if not fetch_date:
            logger.fatal("need fetch date")
            sys.exit(1)

        self.fetch_date = fetch_date

        batch_index = self.args.batch_index
        if batch_index is None:
            logger.fatal("need batch index")
            sys.exit(1)
        self.batch_index = batch_index

    def main_loop(self) -> None:
        assert self.connection
        chan = self.connection.channel()

        batch = []
        logger.info(f"Loading batch {self.batch_index} from disk")

        data_path = DATAPATH_BY_DATE(self.fetch_date)
        batch_path = data_path + f"batch_{self.batch_index}.csv"
        with open(batch_path, "r") as batch_file:
            batch_reader = csv.DictReader(batch_file)
            for row in batch_reader:
                batch.append(str.encode(row["serialized_story"]))

        for entry in batch:
            story = DiskStory.load(entry)
            http_meta = story.http_meta()
            if http_meta.response_code == 200:
                self.send_message(chan, story.dump())


if __name__ == "__main__":
    app = BatchQueuer(
        "BatchQueuer",
        "Reads in a batch after the HTMLFetcher has run, and if the html content is present, enqueues it into rabbitmq",
    )
    app.main()
