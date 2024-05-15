"""
Read CSVs of URLS from legacy system in S3, between the dates, 2022 01/25 - 02/17
"""

import csv
import datetime as dt
import io
import logging
import re
from typing import BinaryIO, Set

from indexer.app import run
from indexer.queuer import Queuer
from indexer.story import StoryFactory

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

        # The legacy system downloaded multiple copies of a story (if
        # seen in different RSS feeds in different sources?).  Looking
        # at 2023-11-04/05 CSV files, 49% of URLs in the file appear
        # more than once.  Since the new system files stories by final
        # URL, downloading multiple copies is a waste (subsequent
        # copies rejected as dups), so try to filter them out WITHIN a
        # single CSV file.
        urls_seen: Set[str] = set()

        # only url column
        for row in csv.reader(io.TextIOWrapper(fobj)):
            logger.debug("%r", row)

            url = row[0]
            if not isinstance(url, str) or not url:
                self.incr_stories("bad-url", repr(url))
                continue

            if url in urls_seen:
                self.incr_stories("dups", url)
                continue

            if not self.check_story_url(url):
                continue  # logged and counted

            # let hist-fetcher quarantine if bad

            story = Story()
            with story.rss_entry() as rss:
                # store downloads_id (used to download HTML from S3)
                # as the "original" location, for use by hist-fetcher.py
                rss.link = url
                rss.fetch_date = fetch_date
                rss.source_feed_id = None
                rss.source_source_id = None  # media is legacy name
                rss.via = fname
                rss.source_url = url

            # collect_date = row.get("collect_date", None)
            # with story.http_metadata() as hmd:
            #     hmd.final_url = url
            #     if collect_date:
            # convert original date/time (treat as UTC) to a
            # timestamp of the time the HTML was fetched,
            # to preserve this otherwise unused bit of information.

            # if len(collect_date) < 20:
            #     collect_date += ".0"  # ensure fraction present

            # fromisoformat wants EXACTLY six digits of fractional
            # seconds, but the CSV files omit trailing zeroes, so
            # use strptime.  Append UTC timezone offset to prevent
            # timestamp method from treating naive datetime as in
            # the local time zone.  date/time stuff and time zones
            # are always a pain, but somehow, this particular
            # corner of Python seems particularly painful.
            # collect_dt = dt.datetime.strptime(
            #     collect_date + " +00:00", "%Y-%m-%d %H:%M:%S.%f %z"
            # )
            # hmd.fetch_timestamp = collect_dt.timestamp()

            # content_metadata.parsed_date is not set, so parser.py will
            # put in the actual parse time as usual:
            # https://github.com/mediacloud/story-indexer/issues/213#issuecomment-1908583666

            self.send_story(story)  # calls incr_story: to increment and log
            urls_seen.add(url)  # mark URL as seen


if __name__ == "__main__":
    run(
        CSVQueuer,
        "csv-queuer",
        "Read CSV of historical stories, queue to hist-fetcher",
    )
