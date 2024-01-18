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
import gzip
import html
import io
import logging
import sys
import time
import xml.sax
from typing import BinaryIO, Callable, List, Optional

import requests

from indexer.app import App, run
from indexer.queuer import Queuer
from indexer.story import BaseStory, StoryFactory
from indexer.storyapp import StoryProducer, StorySender

S3_URL_BASE = "https://mediacloud-public.s3.amazonaws.com/backup-daily-rss"

Attrs = xml.sax.xmlreader.AttributesImpl

Story = StoryFactory()

logger = logging.getLogger("queue-rss")


class RSSHandler(xml.sax.ContentHandler):
    def __init__(self, app: "RSSQueuer"):
        self.app = app
        self.parsed = 0
        self.in_item = False
        self.link = ""
        self.domain = ""
        self.pub_date = ""
        self.title = ""

    def startElement(self, name: str, attrs: Attrs) -> None:
        # error if in_item is True (missing end element?)
        if name == "item":
            self.in_item = True
        self.content: List[str] = []

    def characters(self, content: str) -> None:
        """
        text content inside current tag
        may come in multiple calls,
        even if/when no intervening element
        """
        # save channel.lastBuildDate for fetch date?
        self.content.append(content)

    def endElement(self, name: str):  # type: ignore[no-untyped-def]
        if not self.in_item:
            return

        # here at end of a tag inside an <item>

        # join bits of tag content together:
        content = "".join(self.content)

        if name == "link":
            # undo HTML entity escapes:
            self.link = html.unescape(content).strip()
        elif name == "domain":
            self.domain = content.strip()
        elif name == "pubDate":
            self.pub_date = content.strip()
        elif name == "title":
            self.title = content.strip()
        elif name == "item":
            if self.link and self.domain:
                s = Story()
                with s.rss_entry() as rss:
                    rss.link = self.link
                    rss.domain = self.domain
                    rss.pub_date = self.pub_date
                    rss.title = self.title
                    # also rss.fetch_date

                self.app.send_story(s)

                self.link = self.domain = self.pub_date = self.title = ""
                self.parsed += 1
            else:
                assert self.app.args
                # don't muddy the water if just a dry-run:
                if not self.app.args.dry_run:
                    self.app.incr_stories("bad", self.link)
                logger.warning(
                    "incomplete item: link: %s domain: %s", self.link, self.domain
                )
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
        xml.sax.parse(fobj, RSSHandler(self))


if __name__ == "__main__":
    run(RSSQueuer, "rss-queuer", "parse and queue rss-fetcher RSS entries")
