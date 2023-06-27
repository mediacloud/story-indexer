import argparse
import csv
import logging
import sys
from typing import Any, Dict, List, Optional, Tuple, TypedDict

from indexer.app import App
from indexer.path import DATAPATH_BY_DATE
from indexer.story import BaseStory, DiskStory
from indexer.workers.fetcher.rss_utils import batch_rss, fetch_backup_rss

"""
App interface to fetching RSS content from S3, splitting into batches, and preparing the filesystem for the next step
"""

logger = logging.getLogger(__name__)


class RSSBatcher(App):
    fetch_date: str
    sample_size: Optional[int]
    num_batches: int
    init_stories: bool = True

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
            "--sample-size",
            dest="sample_size",
            type=int,
            default=None,
            help="Number of stories to batch. Default (None) is 'all of them'",
        )

        # num_batches
        num_batches_default = 20
        ap.add_argument(
            "--num-batches",
            dest="num_batches",
            type=int,
            default=num_batches_default,
            help=f"Number of batches to break stories into (default {num_batches_default})",
        )

        # init_stories
        ap.add_argument(
            "--init-stories",
            dest="init_stories",
            action=argparse.BooleanOptionalAction,
            default=True,
            help="Toggle initialization of story objects- if on (by default), this script initializes the data directory for each story and only passes serialized story content in the batchfile. If off, batchfiles contain the whole rss_entry content",
        )

    def process_args(self) -> None:
        super().process_args()

        assert self.args
        fetch_date = self.args.fetch_date
        if not fetch_date:
            logger.fatal("need fetch date")
            sys.exit(1)

        self.fetch_date = fetch_date

        self.sample_size = self.args.sample_size
        self.num_batches = self.args.num_batches
        self.init_stories = self.args.init_stories

    def main_loop(self) -> None:
        rss_records = fetch_backup_rss(self.fetch_date, self.sample_size)
        batches, batch_map = batch_rss(rss_records, num_batches=self.num_batches)

        data_path = DATAPATH_BY_DATE(self.fetch_date)

        for batch_index, batch in enumerate(batches):
            batch_path = data_path + f"batch_{batch_index}.csv"
            with open(batch_path, "w") as batch_file:
                if self.init_stories:
                    header = ["serialized_story"]
                else:
                    header = batch[0].keys()
                writer = csv.DictWriter(batch_file, fieldnames=header)
                writer.writeheader()
                for story in batch:
                    if self.init_stories:
                        new_story: DiskStory = DiskStory()
                        with new_story.rss_entry() as rss_entry:
                            rss_entry.link = story["link"]
                            rss_entry.title = story["title"]
                            rss_entry.domain = story["domain"]
                            rss_entry.pub_date = story["pub_date"]
                            rss_entry.fetch_date = self.fetch_date
                        writer.writerow(
                            {"serialized_story": new_story.dump().decode("utf8")}
                        )
                    else:
                        writer.writerow(story)

        # This might not be neccesary, but keeping it around for now.
        batch_map_path = data_path + "batch_map.csv"
        with open(batch_map_path, "w") as map_file:
            header = ["domain", "batch_index"]
            writer = csv.DictWriter(map_file, fieldnames=header)
            writer.writeheader()
            for domain, index in batch_map.items():
                writer.writerow({"domain": domain, "batch_index": index})


if __name__ == "__main__":
    app = RSSBatcher(
        "RSS_Batcher",
        "Fetches RSS from S3, breaks into batches, and (optionally) sets up disk storage for future pipeline steps",
    )
    app.main()
