import argparse
import csv
import logging
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, TypedDict

from indexer.app import App
from indexer.path import DATAPATH_BY_DATE
from indexer.story import BaseStory, StoryFactory
from indexer.workers.fetcher.rss_utils import RSSEntry, batch_rss, fetch_daily_rss

"""
App interface to fetching RSS content from S3, splitting into batches, and preparing the filesystem for the next step
"""

logger = logging.getLogger(__name__)

Story = StoryFactory()


class RSSBatcher(App):
    fetch_date: str
    sample_size: Optional[int]
    num_batches: int

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

    def main_loop(self) -> None:
        rss_records = fetch_daily_rss(self.fetch_date, self.sample_size)
        batches, batch_map = batch_rss(rss_records, num_batches=self.num_batches)

        data_path = DATAPATH_BY_DATE(self.fetch_date)

        for batch_index, batch in enumerate(batches):
            batch_path = data_path + f"batch_{batch_index}"
            Path(batch_path).mkdir(parents=True, exist_ok=True)
            with open(batch_path + ".csv", "w") as batch_file:
                header = batch[0].keys()
                writer = csv.DictWriter(batch_file, fieldnames=header)
                writer.writeheader()
                for story in batch:
                    writer.writerow(story)

                    new_story: BaseStory = Story()
                    with new_story.rss_entry() as rss_entry:
                        rss_entry.link = story["link"]
                        rss_entry.title = story["title"]
                        rss_entry.domain = story["domain"]
                        rss_entry.pub_date = story["pub_date"]
                        rss_entry.fetch_date = story["fetch_date"]

                    uuid = new_story.uuid()
                    assert isinstance(uuid, str)
                    save_loc = batch_path + "/" + uuid
                    Path(save_loc).write_bytes(new_story.dump())

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
