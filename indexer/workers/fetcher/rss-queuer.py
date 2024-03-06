"""
Queuer to read rss-fetcher synthetic RSS file from rss-fetcher
and queue for a fetcher.

Takes any number local files, http/https/s3 urls
plus either --yesterday or --days N

Queuer framework handles local files, http/https/s3 URLs,
transparently un-gzips.

On-the-fly XML parsing using "SAX" parser.

NOTE! --yesterday only valid after 01:00 GMT
(before that, gets the day before)
"""

import argparse
import html
import logging
import time
import xml.sax
from typing import BinaryIO, List, Optional

from indexer.app import run
from indexer.queuer import Queuer
from indexer.story import StoryFactory

S3_URL_BASE = "https://mediacloud-public.s3.amazonaws.com/backup-daily-rss"

Attrs = xml.sax.xmlreader.AttributesImpl

Story = StoryFactory()

logger = logging.getLogger("queue-rss")


def optional_int(input: Optional[str]) -> Optional[int]:
    if not input or not input.isdigit():
        return None
    return int(input)


class RSSHandler(xml.sax.ContentHandler):
    link: str  # required!
    domain: Optional[str]
    pub_date: Optional[str]
    title: Optional[str]
    source_url: Optional[str]
    source_feed_id: Optional[int]
    source_source_id: Optional[int]
    content: List[str]

    def __init__(self, app: "RSSQueuer", fname: str):
        self.app = app
        self.parsed = self.bad = 0
        self.in_item = False
        self.file_name = fname
        self.reset_item()
        self.content = []

    def reset_item(self) -> None:
        self.link = ""
        self.domain = ""
        self.pub_date = ""
        self.title = ""
        self.source_url = None
        self.source_feed_id = None
        self.source_source_id = None

    def startElement(self, name: str, attrs: Attrs) -> None:
        if name == "item":
            # error if in_item is True (missing end element?)
            self.in_item = True
        elif self.in_item:
            if name == "source":
                self.source_url = attrs.get("url")
                self.source_feed_id = optional_int(attrs.get("mcFeedId"))
                self.source_source_id = optional_int(attrs.get("mcSourceId"))
        self.content = []  # DOES NOT WORK FOR NESTED TAGS!

    def characters(self, content: str) -> None:
        """
        handle text content inside current tag;
        may come in multiple calls!!
        DOES NOT WORK FOR NESTED TAGS!!
        (would need a stack pushed by startElement, popped by endElement)
        """
        self.content.append(content)

    def endElement(self, name: str):  # type: ignore[no-untyped-def]
        if not self.in_item:
            return

        # here at end of a tag inside an <item>

        # join bits of tag content together
        # DOES NOT WORK FOR NESTED TAGS!!
        content = "".join(self.content)

        if name == "link":
            # undo HTML entity escapes:
            self.link = html.unescape(content).strip()
        elif name == "domain":
            self.domain = content.strip() or None
        elif name == "pubDate":
            self.pub_date = content.strip() or None
        elif name == "title":
            self.title = content.strip() or None
        elif name == "item":
            # domain not required by queue-based fetcher
            if self.link:
                s = Story()
                # mypy reval_type(rss) in "with s.rss_entry() as rss" gives Any!!
                rss = s.rss_entry()
                with rss:
                    rss.link = self.link
                    rss.domain = self.domain
                    rss.pub_date = self.pub_date
                    rss.title = self.title
                    rss.source_url = self.source_url
                    rss.source_feed_id = self.source_feed_id
                    rss.source_source_id = self.source_source_id
                    rss.via = self.file_name  # instead of fetch_date
                self.app.send_story(s)
                self.reset_item()
                self.parsed += 1
            else:
                assert self.app.args
                # don't muddy the water if just a dry-run:
                if not self.app.args.dry_run:
                    self.app.incr_stories("bad", self.link)
                    self.bad += 1
            self.in_item = False


class RSSQueuer(Queuer):
    HANDLE_GZIP = True  # transparently handle .gz files

    def __init__(self, process_name: str, descr: str):
        super().__init__(process_name, descr)

        self.sample_size: Optional[int] = None
        self.dry_run = False

    def define_options(self, ap: argparse.ArgumentParser) -> None:
        super().define_options(ap)

        # handle any number of local/http/s3 files on command line,
        # plus some short-cuts for loading s3 files from the usual place

        # take any number of specific dates
        ap.add_argument(
            "--fetch-date",
            action="extend",
            nargs="*",
            help="Date (in YYYY-MM-DD) to fetch",
        )

        # optional: one of --yesterday or --days N
        group = ap.add_mutually_exclusive_group()
        group.add_argument(
            "--days",
            type=int,
            help="number of past days to fetch",
        )
        # equivalent to --days=1
        group.add_argument(
            "--yesterday",
            action="store_true",
            default=False,
            help="fetch yesterday's RSS file (after 01:00GMT)",
        )

    def process_args(self) -> None:
        super().process_args()

        args = self.args
        assert args

        def add_by_date(date: str) -> None:
            args.input_files.append(f"{S3_URL_BASE}/mc-{date}.rss.gz")

        def previous(days: int) -> str:
            """
            get date of rss-fetcher file from "days" days ago
            rss-fetcher output usually ready by 00:45 GMT
            before then, use previous day!!
            """
            return time.strftime(
                "%Y-%m-%d", time.gmtime(time.time() - (days * 24 + 1) * 60 * 60)
            )

        def add_previous(days: int) -> None:
            add_by_date(previous(days))

        self.yesterday = previous(1)  # for range check

        if args.fetch_date:  # handle --fetch-date option(s)
            for date in args.fetch_date:
                if len(date) != 10 or date > self.yesterday or date < "2022-02-18":
                    logger.error("bad date %s", date)
                else:
                    add_by_date(date)

        if args.yesterday:
            add_previous(1)
        elif args.days is not None:
            # work backwards from yesterday
            for days in range(1, args.days + 1):
                add_previous(days)

    def process_file(self, fname: str, fobj: BinaryIO) -> None:
        """
        called for each file/url on command line, and those
        implied by --fetch-date, --days and --yesterday
        with an uncompressed (binary) byte stream
        """
        handler = RSSHandler(self, fname)
        xml.sax.parse(fobj, handler)
        logger.info("processed %s: %d ok, %d bad", fname, handler.parsed, handler.bad)


if __name__ == "__main__":
    run(RSSQueuer, "rss-queuer", "parse and queue rss-fetcher RSS entries")
