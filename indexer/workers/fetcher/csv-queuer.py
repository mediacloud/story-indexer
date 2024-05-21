"""
Read CSVs of URLS from legacy system in S3, between the dates, 2022 01/25 - 02/17
"""

import csv
import io
import logging
import re
from typing import BinaryIO, Set

from indexer.app import run
from indexer.queuer import Queuer
from indexer.story import RSSEntry, StoryFactory

logger = logging.getLogger(__name__)

Story = StoryFactory()

# regular expression to try to extract date from CSV file name:
DATE_RE = re.compile(r"(\d\d\d\d)[_-](\d\d)[_-](\d\d)")


class CSVQueuer(Queuer):
    AWS_PREFIX = "HIST"  # S3 env var prefix
    HANDLE_GZIP = True  # just in case

    def process_file(self, fname: str, fobj: BinaryIO) -> None:
        """
        called for each input file with open binary/bytes I/O object
        """
        # try extracting date from file name to create fetch_date for RSSEntry.
        m = DATE_RE.match(fname)
        if m:
            fetch_date = "-".join(m.groups())
        else:
            fetch_date = None

        urls_seen: Set[str] = set()

        # only url column
        for row in csv.reader(io.TextIOWrapper(fobj)):
            logger.debug("%r", row)

            url = row[0]
            if not self.check_story_url(url):
                continue  # logged and counted

            if url in urls_seen:
                self.incr_stories("dups", url)
                continue

            story = Story()
            rss: RSSEntry = story.rss_entry()
            with rss:
                rss.link = url
                rss.fetch_date = fetch_date
                rss.via = fname

            with story.http_metadata() as hmd:
                hmd.final_url = url

            self.send_story(story)  # calls incr_story: to increment and log
            urls_seen.add(url)  # mark URL as seen


if __name__ == "__main__":
    run(
        CSVQueuer,
        "csv-queuer",
        "Read CSV of historical stories, queue to regular fetcher",
    )
