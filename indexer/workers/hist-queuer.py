"""
Read CSVs of articles from legacy system from S3, http or local
files and queues Stories with URL and legacy downloads_id for
hist-fetcher (S3 fetch latency is too high for a single process to
achieve anything close to S3 request rate limit (5500 requests/second
per prefix)
"""

import csv
import datetime as dt
import io
import logging
import re
from typing import BinaryIO

from indexer.app import run
from indexer.queuer import Queuer
from indexer.story import StoryFactory

logger = logging.getLogger(__name__)

Story = StoryFactory()

DATE_RE = re.compile(r"(\d\d\d\d)[_-](\d\d)[_-](\d\d)")


class HistQueuer(Queuer):
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
            fetch_date = fname  # better than nothing?

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

            story = Story()
            with story.rss_entry() as rss:
                # store downloads_id (used to download HTML from S3)
                # as the "original" location, for use by hist-fetcher.py
                rss.link = dlid
                rss.fetch_date = fetch_date

            collect_date = row.get("collect_date", None)
            with story.http_metadata() as hmd:
                hmd.final_url = url
                if collect_date:
                    # convert original date/time (treat as UTC) to a
                    # timestamp of the time the HTML was fetched,
                    # to preserve this otherwise unused bit of information.

                    # fromisoformat wants EXACTLY six digits of fractional
                    # seconds, but the CSV files omit trailing zeroes, so
                    # use strptime.  Append UTC timezone offset to prevent
                    # timestamp method from treating naive datetime as in
                    # the local time zone.  date/time stuff and time zones
                    # are always a pain, but somehow, this particular
                    # corner of Python seems particularly painful.
                    collect_dt = dt.datetime.strptime(
                        collect_date + " +00:00", "%Y-%m-%d %H:%M:%S.%f %z"
                    )
                    hmd.fetch_timestamp = collect_dt.timestamp()

            lang = row.get("language", None)
            if lang:
                with story.content_metadata() as cmd:
                    cmd.language = cmd.full_language = lang

            # content_metadata.parsed_date is not set, so parser.py will
            # put in the actual parse time as usual:
            # https://github.com/mediacloud/story-indexer/issues/213#issuecomment-1908583666

            self.send_story(story)  # increments counter


if __name__ == "__main__":
    run(
        HistQueuer,
        "hist-queuer",
        "Read CSV of historical stories, queue to hist-fetcher",
    )
