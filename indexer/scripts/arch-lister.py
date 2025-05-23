"""
Sketch of an "archive eraser"

Reads archive files (possibly from remote "blob stores"), extracts URLs
and removes objects from Elasticsearch.

NOTE!! Does not actually remove the archive files from their
off-site storage location(s)!!!!

This was written to remove stories for late Jan thru early March 2022
(database E) that were initially recovered in 2024 (with some link
rot) thru various means(*), so that stories "blindly" recovered from
S3 (without known URL) where canonical URLs were then extracted.

However, experimention suggested that loading the canonical URLs
without first removing the first attempt could lead to a 10% duplicate
rate (initial vs. final URLs and other URL differences).

From the below, it appears all of the WARC files are available on S3,
with some available from B2 as well (may be cheaper to fetch from B2).

(*) The different ways the URLs were recovered:
1. From synthetic RSS files written at the time for IA (both by the legacy system
   and the then "backup" rss-fetcher.

2. From RSS files blindly extracted from S3 (ignoring the HTML files!) into CSV files of URLs

Stories in index mc_search-00002 and mc_search-00003

All files on S3, some on B2 (starting 2024/05/31)

arch prefix     start           end             archives
mccsv           2024/05/22 -> 2024/06/27        S3/(B2)
mc(rss)         2024/05/27 -> 2024/06/20        S3/(B2) [1]
mcrss           2024/06/20 -> 2024/08/16        S3/B2

(B2) means some of the date range on B2

[1] initial WARC files from RSS files written from 2024/05/27 thru
    2024/06/20 start with mc- (see below):

    THESE SHOULD BE VERIFIED!!! The "via" field in the metadata
    should indicate how the URL was obtained!

    dates                   container name (in WARC filename)
    2024/05/27-2024/05/28   cf94b52abe5a        S3 [154 files]
    2024/05/29-2024/06/04   cefd3fdce464        S3 [882 files]
    2024/06/05              0c501ed61cf4        S3 & B2 [497 files]
    2024/06/05              446d55936e82        S3 & B2 [27 files]
    2024/06/05              cefd3fdce464
    2024/06/06-2024/06/09   0c501ed61cf4
    2024/06/09              7e1b47c305f1        S3 & B2 [1 file]
    2024/06/11-2024/06/20   6c55aaf9daaa

================

This is based on the "Queuer" class, which reads both local and remote
input files, and keeps track of which files have been processed.

No queues are involved (provide any value for --rabbitmq-url or RABBITMQ_URL)

The "tracker" uses SQLite3 (**), and should be multi-process safe,
although this application may experience more contention (SQLite3 does
full-table locks for row creation), and testing should be done (using
--dry-run) with multiple processes running to see if any errors or
exceptions are thrown due to lock contention!

(**) The author doesn't care how you pronounce it, but he says "ess cue ell ite"
(like it's a mineral): https://www.youtube.com/watch?v=Jib2AmRb_rk

Because the files involved span a wide range of dates, and have
various forms, rather than implement fancy wildcard or filtering
support, the idea is to collect all the (full) archive URLs into a
file (or files), and use the "indirect file" feature in the queuer
to read the files of URLs.
"""

import argparse
import logging
import os
from contextlib import nullcontext
from io import TextIOWrapper
from typing import BinaryIO, List, Optional, TextIO, Union

from indexer.path import app_data_dir
from indexer.queuer import Queuer
from indexer.story_archive_writer import StoryArchiveReader

logger = logging.getLogger("arch-lister")


class ArchLister(Queuer):
    APP_BLOBSTORE = "HIST"  # first choice for blobstore conf vars
    HANDLE_GZIP = False  # StoryArchiveReader handles compression

    # don't want to talk to RabbitMQ, but too much work
    # to refactor Queuer into a FileProcessor add-in

    def __init__(self, process_name: str, descr: str):
        super().__init__(process_name, descr)
        self.url_output_dir = app_data_dir(os.path.join(self.process_name, "url_list"))
        self.warc_output_dir = app_data_dir(
            os.path.join(self.process_name, "warc_list")
        )

    def qconnect(self) -> None:
        return

    def check_output_queues(self) -> None:
        return

    def define_options(self, ap: argparse.ArgumentParser) -> None:
        super().define_options(ap)
        ap.add_argument(
            "-o",
            dest="output_file",
            help="The path to the output file where the archive list will be written",
        )
        ap.add_argument(
            "-w",
            dest="list_warc_files_only",
            action="store_true",
            help="Write list of discovered WARC files to the specified output file (-o) without processing their contents.",
        )

    def process_args(self) -> None:
        """
        Process command line arguments and set instance variables.
        """
        super().process_args()
        assert self.args

    def process_file(self, fname: str, fobj: BinaryIO) -> None:
        assert self.args
        logger.info("process_file %s", fname)
        # it may be possible to make this faster by NOT using
        # StoryArchiveReader and warcio, but it came for "free":
        reader = StoryArchiveReader(fobj)
        urls = []
        for story in reader.read_stories():
            urls.append(story.content_metadata().url)
        logger.info("collected %d urls from %s", len(urls), fname)
        if self.args.dry_run:
            return

        # Not a dry run, write to the actual output file
        logger.warning("marked %d urls for deletion %s here!", len(urls), fname)
        if self.args.output_file:
            output_path = self.args.output_file
        else:
            output_path = os.path.join(
                self.url_output_dir, f"{fname.split('/')[-1]}.txt"
            )
        self.write_to_output_file(urls, output_path)

    def maybe_process_file(self, fname: str) -> None:
        assert self.args
        if self.args.list_warc_files_only:
            self.write_to_output_file(
                fname, os.path.join(self.warc_output_dir, self.args.output_file)
            )
        else:
            super().maybe_process_file(fname)

    def write_to_output_file(
        self, content: Union[str, List[Optional[str]]], output_file_path: str | None
    ) -> None:
        if output_file_path:
            output_file: Union[TextIO, nullcontext] = open(output_file_path, "a")
        else:
            output_file = nullcontext()

        with output_file as file:
            if isinstance(file, TextIOWrapper):
                if isinstance(content, list):
                    logger.info(
                        "writing %d files to %s", len(content), output_file_path
                    )
                    for item in content:
                        if item is not None:
                            file.write(f"{item}\n")
                            logger.info("writing %s to %s", item, output_file_path)
                else:
                    logger.info("writing %s to %s", content, output_file_path)
                    file.write(f"{content}\n")


if __name__ == "__main__":
    app = ArchLister(
        "arch-lister",
        "generates a list of stories from archive files for removal from ES.",
    )
    app.main()
