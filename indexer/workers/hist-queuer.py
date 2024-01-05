"""
Read CSV of articles from legacy system, make queue entry for
hist-fetcher (S3 fetch latency is high enough to prevent reading from
S3 at full rate with a single fetcher)

XXX Toss stories w/ domain name ending in member of NON_NEWS_DOMAINS
"""

import argparse
import csv
import io
import logging
import os
import sys
from typing import IO, cast

from mcmetadata.urls import NON_NEWS_DOMAINS

from indexer.app import run
from indexer.queuer import Queuer
from indexer.story import BaseStory, StoryFactory

logger = logging.getLogger(__name__)

Story = StoryFactory()


class HistQueuer(Queuer):
    def process_file(self, fname: str, fobj: io.IOBase) -> None:
        # typical columns:
        # collect_date,stories_id,media_id,downloads_id,feeds_id,[language,]url
        fobj2 = cast(IO[bytes], fobj)
        for row in csv.DictReader(io.TextIOWrapper(fobj2)):
            logger.debug("%r", row)

            url = row.get("url", None)
            if not url:
                logger.error("no url: %r", row)
                self.incr_stories("no-url")
                continue

            # XXX extract FQDN, check if in non-news domain

            dlid = row.get("downloads_id", None)
            if not dlid or not dlid.isdigit():
                logger.error("bad downloads_id: %r", row)
                self.incr_stories("bad-dlid")
                continue

            collect_date = row.get("collect_date", None)

            story = Story()
            with story.rss_entry() as rss:
                rss.link = dlid  # could pass S3 url
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
