"""
Read CSVs of articles from legacy system from S3, http or local
files and queues Stories with URL and legacy downloads_id for
hist-fetcher (S3 fetch latency is too high for a single process to
achieve S3 request rate limit (5500 requests/second per prefix)
"""

import argparse
import csv
import io
import logging
import os
import sys
from typing import BinaryIO

from indexer.app import run
from indexer.queuer import Queuer
from indexer.story import BaseStory, StoryFactory

logger = logging.getLogger(__name__)

Story = StoryFactory()


class HistQueuer(Queuer):
    AWS_PREFIX = "HIST"  # S3 env var prefix
    HANDLE_GZIP = True  # just in case

    def process_file(self, fname: str, fobj: BinaryIO) -> None:
        """
        called for each input file with open binary/bytes I/O object
        """
        # typical columns:
        # collect_date,stories_id,media_id,downloads_id,feeds_id,[language,]url
        for row in csv.DictReader(io.TextIOWrapper(fobj)):
            logger.debug("%r", row)

            url = row.get("url", "")
            if not self.check_story_url(url):
                continue  # logged and counted

            dlid = row.get("downloads_id", None)
            if not dlid or not dlid.isdigit():
                logger.error("bad downloads_id: %r", row)
                self.incr_stories("bad-dlid", url)
                continue

            collect_date = row.get("collect_date", None)

            story = Story()
            with story.rss_entry() as rss:
                rss.link = dlid  # could pass as S3 url!
                if collect_date:
                    rss.fetch_date = collect_date

            with story.http_metadata() as hmd:
                hmd.final_url = url

            lang = row.get("language", None)
            if lang:
                with story.content_metadata() as cmd:
                    cmd.language = cmd.full_language = lang

            self.send_story(story)  # increments counter


if __name__ == "__main__":
    run(
        HistQueuer,
        "hist-queuer",
        "Read CSV of historical stories, queue to hist-fetcher",
    )
