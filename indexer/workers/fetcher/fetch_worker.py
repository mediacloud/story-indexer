import argparse
import csv
import logging
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, TypedDict

import scrapy.utils.log
from mcmetadata.urls import NON_NEWS_DOMAINS
from scrapy.crawler import CrawlerProcess

from indexer.app import run
from indexer.story import BaseStory, StoryFactory
from indexer.storyapp import StoryProducer, StoryWorker
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


class FetchWorker(StoryProducer):
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

        if http_meta.final_url is None:
            status_label = "no-url"  # was an assert
        elif http_meta.response_code is None:
            status_label = "no-resp"
        elif http_meta.response_code == 200:
            if len(story.dump()) > MAX_FETCHER_MSG_SIZE:
                logger.warn(
                    f"Story over {MAX_FETCHER_MSG_SIZE} limit: {story.rss_entry().link}, size: {len(story.dump())}"
                )
                status_label = "oversized"
            elif any(dom in http_meta.final_url for dom in NON_NEWS_DOMAINS):
                status_label = "non-news"
            else:
                if not hasattr(self, "sender"):
                    self.qconnect()
                    self.sender = self.story_sender()
                self.sender.send_story(story)
                status_label = "success"

        elif http_meta.response_code in (403, 404, 429):
            status_label = f"http-{http_meta.response_code}"
        else:
            status_label = f"http-{http_meta.response_code//100}xx"

        self.incr_stories(status_label, http_meta.final_url or "")

    def main_loop(self) -> None:
        # Fetch and batch rss
        logger.info(f"Fetching rss batch {self.batch_index} for {self.fetch_date}")
        all_rss_records = fetch_daily_rss(self.fetch_date, self.sample_size)
        batches, batch_map = batch_rss(all_rss_records, num_batches=self.num_batches)
        self.rss_batch = batches[self.batch_index]

        # Logging batch information:
        for i in range(self.num_batches):
            batch = batches[i]
            batch_size = len(batch[i])
            domains = len(set([s["domain"] for s in batch]))
            logger.info(
                f"Batch {i}:  {batch_size} stories, from {domains} domains (~{batch_size / domains} stories per domain"
            )

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

        # UGH!! scrapy.utils.log.configure_logging, called from
        # CrawlerProcess constructor calls
        # logging.dictConfig(DEFAULT_LOGGING) which closes all
        # existing handlers (ie; the SysLogHandler), so wack the
        # config to be "incremental"!!!
        scrapy.utils.log.DEFAULT_LOGGING["incremental"] = True

        # Fetch html as stories

        # install_root_handler=False keeps scrapy from installing ANOTHER stderr handler!
        process = CrawlerProcess(install_root_handler=False)
        logger.info(f"Launching Batch Spider Process for Batch {self.batch_index}")
        process.crawl(BatchSpider, batch=self.stories_to_fetch, cb=self.scrapy_cb)
        process.start()

        logger.info(
            f"Fetched {len(self.fetched_stories)} stories in batch {self.batch_index}"
        )


if __name__ == "__main__":
    run(
        FetchWorker,
        "fetcher",
        "Reads the rss_fetcher's content, batches it, initializes story objects, fetches a batch, then enqueues it into rabbitmq",
    )
