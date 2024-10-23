"""
Read CSVs of articles from legacy system from S3, http or local
files and queues Stories with URL and legacy downloads_id for
hist-fetcher (S3 fetch latency is too high for a single process to
achieve anything close to S3 request rate limit (5500 requests/second
per prefix)
"""

import argparse
import csv
import datetime as dt
import io
import logging
import re
from typing import BinaryIO, Set

from indexer.app import run
from indexer.queuer import Queuer
from indexer.story import NEED_CANONICAL_URL, StoryFactory

logger = logging.getLogger(__name__)

Story = StoryFactory()

# regular expression to try to extract date from CSV file name:
DATE_RE = re.compile(r"(\d\d\d\d)[_-](\d\d)[_-](\d\d)")


class HistQueuer(Queuer):
    APP_BLOBSTORE = "HIST"  # first choice for blobstore/keys
    HANDLE_GZIP = True  # just in case
    SHUFFLE_BATCH_SIZE = 0  # uses hist-fetcher no shuffling needed

    def define_options(self, ap: argparse.ArgumentParser) -> None:
        super().define_options(ap)

        ap.add_argument(
            "--allow-no-url",
            action="store_true",
            default=False,
            help="Allow CSV's without URL (depend on canonical URL extraction)",
        )

    def process_file(self, fname: str, fobj: BinaryIO) -> None:
        """
        called for each input file with open binary/bytes I/O object
        """
        assert self.args
        url_required = not self.args.allow_no_url

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

        # typical columns:
        # collect_date,stories_id,media_id,downloads_id,feeds_id,[language,]url
        for row in csv.DictReader(io.TextIOWrapper(fobj)):
            logger.debug("%r", row)

            url = row.get("url")
            if url_required:
                if not isinstance(url, str) or not url:
                    self.incr_stories("bad-url", repr(url))
                    continue

                if url in urls_seen:
                    self.incr_stories("dups", url)
                    continue

                if not self.check_story_url(url):
                    continue  # logged and counted
            else:
                url = NEED_CANONICAL_URL

            dlid = row.get("downloads_id")
            # let hist-fetcher quarantine if bad

            # convert to int: OK if missing or malformed
            try:
                feeds_id = int(row["feeds_id"])
            except (KeyError, ValueError):
                # XXX cannot count w/ incr_stories (only incremented once per story)
                feeds_id = None

            # convert to int: OK if missing or malformed
            try:
                media_id = int(row["media_id"])
            except (KeyError, ValueError):
                # XXX cannot count w/ incr_stories (only incremented once per story)
                media_id = None

            story = Story()
            with story.rss_entry() as rss:
                # store downloads_id (used to download HTML from S3)
                # as the "original" location, for use by hist-fetcher.py
                rss.link = dlid
                rss.fetch_date = fetch_date
                rss.source_feed_id = feeds_id
                rss.source_source_id = media_id  # media is legacy name
                rss.via = fname

            collect_date = row.get("collect_date", None)
            with story.http_metadata() as hmd:
                hmd.final_url = url
                if collect_date:
                    # convert original date/time (treat as UTC) to a
                    # timestamp of the time the HTML was fetched,
                    # to preserve this otherwise unused bit of information.

                    if len(collect_date) < 20:
                        collect_date += ".0"  # ensure fraction present

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

            # NOTE! language was extracted from legacy database (where possible) due to
            # Phil's mistaken assumption that language detection was CPU intensive, but
            # that was due to py3lang/numpy/openblas dot operator defaulting to one
            # worker thread per CPU core, and the worker threads all spinning for work,
            # raising the load average.

            # When parser.py is run with OPENBLAS_NUM_THREADS=1, the cpu use is
            # negligable, and the work to extract the language, and to make it possible
            # to pass "override" data in to mcmetadata.extract was for naught.  And it's
            # better to assume that current tools are better than the old ones.

            # Nonetheless, since the data is available, put it in the Story.  It will
            # either be overwritten, or be available if the Story ends up being
            # quarantined by the parser.
            lang = row.get("language", None)
            if lang:
                with story.content_metadata() as cmd:
                    cmd.language = cmd.full_language = lang

            # content_metadata.parsed_date is not set, so parser.py will
            # put in the actual parse time as usual:
            # https://github.com/mediacloud/story-indexer/issues/213#issuecomment-1908583666

            self.send_story(story)  # calls incr_story: to increment and log
            if url != NEED_CANONICAL_URL:
                urls_seen.add(url)  # mark URL as seen


if __name__ == "__main__":
    run(
        HistQueuer,
        "hist-queuer",
        "Read CSV of historical stories, queue to hist-fetcher",
    )
