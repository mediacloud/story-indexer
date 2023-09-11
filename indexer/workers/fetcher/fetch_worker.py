import argparse
import csv
import logging
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, TypedDict

from scrapy.crawler import CrawlerProcess

from indexer.story import BaseStory, StoryFactory, uuid_by_link
from indexer.worker import QApp
from indexer.workers.fetcher.batch_spider import BatchSpider
from indexer.workers.fetcher.rss_utils import RSSEntry, batch_rss, fetch_daily_rss

"""
Worker (Producer) interface which queues stories fetched by the HTML fetcher
"""

logger = logging.getLogger(__name__)

Story = StoryFactory()

MAX_FETCHER_MSG_SIZE: int = (
    10000000  # 10Mb- > 99.99% of pages should fit under this limit.
)


class FetchWorker(QApp):
    AUTO_CONNECT: bool = False

    fetch_date: str
    sample_size: Optional[int]
    num_batches: int
    batch_index: int

    rss_batch: List[RSSEntry] = []
    stories_to_fetch: List[BaseStory] = []
    fetched_stories: List[BaseStory] = []

    def define_options(self, ap: argparse.ArgumentParser) -> None:
        super().define_options(ap)

        ap.add_argument(
            "-y",
            "--yesterday",
            action="store_true",
            default=False,
            help="Flag, if set, to fetch content for yesterday's date at run-time",
        )

        # fetch_date
        ap.add_argument(
            "--fetch-date",
            dest="fetch_date",
            help="Date (in YYYY-MM-DD) to fetch",
        )

        # num_batches
        num_batches_environ = os.environ.get("FETCHER_NUM_BATCHES")
        ap.add_argument(
            "--num-batches",
            dest="num_batches",
            type=int,
            default=num_batches_environ,
            help="Number of batches to break stories into. If not set, defaults to value of FETCHER_NUM_BATCHES environ",
        )

        # batch_index
        ap.add_argument(
            "--batch-index",
            dest="batch_index",
            type=int,
            default=None,
            help="The index of the batch to work on fetching",
        )

        # sample_size
        ap.add_argument(
            "--sample-size",
            dest="sample_size",
            type=int,
            default=None,
            help="Number of stories to batch. Default (None) is 'all of them'",
        )

    def process_args(self) -> None:
        super().process_args()

        assert self.args
        logger.info(self.args)

        if self.args.yesterday:
            logger.info("Fetching for yesterday")
            yesterday = datetime.today() - timedelta(days=2)
            fetch_date = yesterday.strftime("%Y-%m-%d")
        else:
            fetch_date = self.args.fetch_date
            if not fetch_date:
                logger.fatal("need fetch date")
                sys.exit(1)

        self.fetch_date = fetch_date

        num_batches = self.args.num_batches
        if num_batches is None:
            logger.fatal("need num_batches")
            sys.exit(1)
        self.num_batches = num_batches

        batch_index = self.args.batch_index
        if batch_index is None:
            logger.fatal("need batch index")
            sys.exit(1)
        self.batch_index = (
            batch_index - 1
        )  # -1, because docker swarm .task.slot is 1-indexed

        self.sample_size = self.args.sample_size

    def scrapy_cb(self, story: BaseStory) -> None:
        # Scrapy calls this when it's finished grabbing a story
        # NB both successes and failures end up here
        http_meta = story.http_metadata()

        assert http_meta.response_code is not None

        if http_meta.response_code == 200:
            status_label = "success"

        elif http_meta.response_code in (403, 404, 429):
            status_label = f"http-{http_meta.response_code}"
        else:
            status_label = f"http-{http_meta.response_code//100}xx"

        self.incr("fetched-stories", labels=[("status", status_label)])
        self.fetched_stories.append(story)

    def main_loop(self) -> None:
        # Fetch and batch rss
        logger.info(f"Fetching rss batch {self.batch_index} for {self.fetch_date}")
        all_rss_records = fetch_daily_rss(self.fetch_date, self.sample_size)
        batches, batch_map = batch_rss(all_rss_records, num_batches=self.num_batches)
        self.rss_batch = batches[self.batch_index]
        logger.info(f"Found {len(self.rss_batch)} entries for batch {self.batch_index}")

        # Initialize stories
        logger.info(f"Initializing stories for {self.batch_index} on {self.fetch_date}")
        for rss_entry in self.rss_batch:
            new_story = Story()
            with new_story.rss_entry() as story_rss_entry:
                story_rss_entry.link = rss_entry["link"]
                story_rss_entry.title = rss_entry["title"]
                story_rss_entry.domain = rss_entry["domain"]
                story_rss_entry.pub_date = rss_entry["pub_date"]
                story_rss_entry.fetch_date = rss_entry["fetch_date"]

            self.stories_to_fetch.append(new_story)

        self.gauge(
            "rss-stories",
            len(self.stories_to_fetch),
            labels=[("batch", self.batch_index)],
        )

        logger.info(f"Initialized {len(self.stories_to_fetch)} stories")

        # Fetch html as stories
        process = CrawlerProcess()
        logger.info(f"Launching Batch Spider Process for Batch {self.batch_index}")
        process.crawl(BatchSpider, batch=self.stories_to_fetch, cb=self.scrapy_cb)
        process.start()

        logger.info(
            f"Fetched {len(self.fetched_stories)} stories in batch {self.batch_index}"
        )

        # enqueue stories
        self.qconnect()

        assert self.connection
        chan = self.connection.channel()

        queued_stories = 0
        oversized_stories = 0
        for story in self.fetched_stories:
            http_meta = story.http_metadata()

            assert http_meta.response_code is not None

            if http_meta.response_code == 200:
                story_dump = story.dump()
                if len(story_dump) > MAX_FETCHER_MSG_SIZE:
                    # Just log this for now- we might want a less ephemeral record eventually.
                    logger.warn(
                        f"Story over {MAX_FETCHER_MSG_SIZE} limit: {story.rss_entry().link}, size: {len(story_dump)}"
                    )
                    oversized_stories += 1
                else:
                    self.send_message(chan, story_dump)
                    queued_stories += 1

        self.gauge(
            "oversized-stories", oversized_stories, labels=[("batch", self.batch_index)]
        )

        self.gauge(
            "queued-stories", queued_stories, labels=[("batch", self.batch_index)]
        )


if __name__ == "__main__":
    app = FetchWorker(
        "fetcher",
        "Reads the rss_fetcher's content, batches it, initializes story objects, fetches a batch, then enqueues it into rabbitmq",
    )
    app.main()
