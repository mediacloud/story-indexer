"""
Queuer to read rss-fetcher synthetic RSS file from rss-fetcher
and queue for a fetcher.

Takes any number local files, http/https/s3 urls
plus either --yesterday or --days N

Queuer framework handles local files, http/https/s3 URLs,
transparently un-gzips.

On-the-fly XML parsing using iterparse
(w/o reading whole file into memory)

NOTE! --yesterday only valid after 01:00 GMT
(before that, gets the day before)
"""

import argparse
import html
import logging
import time
from typing import BinaryIO

# xml.etree.ElementTree.iterparse doesn't have recover argument
# (fatal error on control characters in element text), so using lxml
from lxml.etree import iterparse

from indexer.app import run
from indexer.queuer import Queuer
from indexer.story import StoryFactory

S3_URL_BASE = "https://mediacloud-public.s3.amazonaws.com/backup-daily-rss"

Story = StoryFactory()

logger = logging.getLogger("rss-queuer")


def optional_int(input: str | None) -> int | None:
    if not input or not input.isdigit():
        return None
    return int(input)


class RSSQueuer(Queuer):
    HANDLE_GZIP = True  # transparently handle .gz files

    def __init__(self, process_name: str, descr: str):
        super().__init__(process_name, descr)
        self.reset_item(True)

    def reset_item(self, first: bool = False) -> None:
        if first:
            self.ok = self.bad = 0
        self.link: str | None = ""
        self.domain: str | None = ""
        self.pub_date: str | None = ""
        self.title: str | None = ""
        self.source_url: str | None = None
        self.source_feed_id: int | None = None
        self.source_source_id: int | None = None

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

    def end_item(self, fname: str) -> None:
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
                rss.via = fname  # instead of fetch_date
            self.send_story(s)
            self.ok += 1
        else:
            assert self.args
            # don't muddy the water if just a dry-run:
            if not self.args.dry_run:
                self.incr_stories("bad", self.link or "no-link")
                self.bad += 1
        self.reset_item()

    def process_file(self, fname: str, fobj: BinaryIO) -> None:
        """
        called for each file/url on command line,
        each file in a directory on the command line,
        each S3 object matching an s3 URL prefix,
        and URLs implied by --fetch-date, --days and --yesterday
        with an uncompressed (binary) byte stream
        """
        path: list[str] = []
        self.reset_item(True)

        # recover=True avoids fatal error when control characters seen in title
        for event, element in iterparse(fobj, events=("start", "end"), recover=True):
            name = element.tag
            logger.debug("%s tag %s level %d", event, name, len(path))
            if event == "start":
                path.append(name)
            else:  # event == "end"
                path.pop()
                if name == "item":
                    self.end_item(fname)
                    continue

                lpath = len(path)
                if (
                    (lpath > 0 and path[0] != "rss")
                    or (lpath > 1 and path[1] != "channel")
                    or (lpath > 2 and path[2] != "item")
                    or lpath > 3
                ):
                    logger.warning(
                        "%s: unexpected tag %s path %s",
                        name,
                        "/".join(path),
                    )
                    continue

                # here at end of a tag inside an <item>
                content = element.text or ""
                assert isinstance(content, str)
                if name == "link":
                    # undo HTML entity escapes:
                    self.link = html.unescape(content).strip()
                elif name == "domain":
                    self.domain = content.strip() or None
                elif name == "pubDate":
                    self.pub_date = content.strip() or None
                elif name == "title":
                    self.title = content.strip() or None
                elif name == "source":
                    self.source_url = element.get("url")
                    self.source_feed_id = optional_int(element.get("mcFeedId"))
                    self.source_source_id = optional_int(element.get("mcSourceId"))
        logger.info("processed %s: %d ok, %d bad", fname, self.ok, self.bad)


if __name__ == "__main__":
    run(RSSQueuer, "rss-queuer", "parse and queue rss-fetcher RSS entries")
