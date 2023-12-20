"""
Read CSV of articles from legacy system, make queue entry for hist-fetcher
(S3 fetch latency is high enough to prevent reading from S3 at full rate)

TODO?
XXX monitor queue length, and sleep between CSV files?
Toss stories w/ domain name ending in member of NON_NEWS_DOMAINS
Take s3 URL for input csv file
Take s3 URL for bucket to read csv files from
Drop empty FILENAME.done turd on S3 for completed files?
"""

import argparse
import csv
import io
import logging
import os
import sys
from typing import Any, Dict, List, Optional

from mcmetadata.urls import NON_NEWS_DOMAINS

from indexer.story import BaseStory, StoryFactory
from indexer.worker import StoryProducer, StorySender, run

logger = logging.getLogger(__name__)

Story = StoryFactory()


class HistQueuer(StoryProducer):
    AUTO_CONNECT: bool = False

    def __init__(self, process_name: str, descr: str):
        super().__init__(process_name, descr)
        self.count = 0

    def define_options(self, ap: argparse.ArgumentParser) -> None:
        super().define_options(ap)
        ap.add_argument("csvs", help="CSV files of stories", nargs="+")
        ap.add_argument(
            "--count", type=int, default=4294967296, help="count of stories to queue"
        )

    def main_loop(self) -> None:
        assert self.args

        self.qconnect()
        sender = self.story_sender()
        self.start_pika_thread()

        self.count = self.args.count
        for csv_fname in self.args.csvs:
            # XXX handle S3 URL for CSV or whole bucket of CSVs
            # XXX check for s3://....csv.done turd first!!
            with open(csv_fname) as f:
                if not self.process_csv(f, sender):
                    break
            # XXX if S3, drop PATH.done turd file?

    def process_csv(self, f: io.TextIOBase, sender: StorySender) -> bool:
        for row in csv.DictReader(f):
            # typ columns: collect_date,stories_id,media_id,downloads_id,feeds_id,[language,]url
            logger.info("%r", row)

            url = row.get("url", None)
            if not url:
                logger.error("no url: %r", row)
                # XXX counter?
                continue

            dlid = row.get("downloads_id", None)
            if not dlid:
                logger.error("bad downloads_id: %r", row)
                # XXX counter?
                continue

            collect_date = row.get("collect_date", None)

            story = Story()
            with story.rss_entry() as rss:
                rss.link = dlid
                rss.fetch_date = collect_date

            with story.http_metadata() as hmd:
                hmd.final_url = url

            lang = row.get("language", None)
            if lang:
                with story.content_metadata() as cmd:
                    cmd.language = cmd.full_language = lang

            # XXX counter?
            sender.send_story(story)

            self.count -= 1
            if self.count <= 0:
                logger.info("reached max count")
                return False
        return True


if __name__ == "__main__":
    run(
        HistQueuer,
        "hist-queuer",
        "Read CSV of historical stories, queue to hist-fetcher",
    )
