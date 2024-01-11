"""
Queuer: base class for programs that read (possibly remote) input
files, processing them, and queuing Stories.

Supports http(s) and S3 file sources, with limited S3
globbing/wildcard (suffix * only).

Skips files that have already been processed using "indexer.tracker"
keep track.

Made into a class because several input paths need the same logic:
1. Historical Ingest: reading CSV files of db dumps, fetching HTTP from S3
2. Queue based fetcher: reading rss-fetcher generated RSS files
3. Reading and replaying WARC archive files

Default "one-file" mode finds at most one file to process and exits,
which is (more) suitable for use from a crontab.

With --loop will loop for all files (and implied files) from command
line (checking queue lengths and sleeping).
"""

import argparse
import gzip
import io
import logging
import os
import random
import sys
import tempfile
import time
from enum import Enum
from typing import TYPE_CHECKING, Any, BinaryIO, List, Optional, cast

import boto3
import botocore
import requests
import urllib3

if TYPE_CHECKING:
    from mypy_boto3_s3.client import S3Client
else:
    S3Client = Any

from indexer.app import AppException
from indexer.story import BaseStory
from indexer.storyapp import StoryProducer, StorySender
from indexer.tracker import TrackerException, get_tracker

logger = logging.getLogger(__name__)


class Queuer(StoryProducer):
    MAX_QUEUE_LEN = 100000  # don't queue if (any) dest queue longer than this

    AWS_PREFIX: str  # prefix for environment vars

    HANDLE_GZIP: bool  # True to intervene if .gz present

    SAMPLE_PERCENT = 0.1  # for --sample-size

    def __init__(self, process_name: str, descr: str):
        super().__init__(process_name, descr)
        self.input_group: Optional[argparse._ArgumentGroup] = None
        self.sender: Optional[StorySender] = None
        self.queued_stories = 0
        self.s3_client_object: Optional[S3Client] = None

    def define_options(self, ap: argparse.ArgumentParser) -> None:
        super().define_options(ap)

        ap.add_argument(
            "--dry-run",
            "-n",
            action="store_true",
            default=False,
            help="don't queue stories",
        )
        ap.add_argument(
            "--force",
            "-f",
            action="store_true",
            default=False,
            help="ignore tracking database (for test)",
        )
        ap.add_argument(
            "--max-queue-len",
            type=int,
            default=self.MAX_QUEUE_LEN,
            help=f"Maximum queue length at which to send a new batch (default: {self.MAX_QUEUE_LEN})",
        )
        ap.add_argument(
            "--max-stories",
            type=int,
            default=None,
            help="Number of stories to queue. Default (None) is 'all of them'",
        )
        ap.add_argument(
            "--loop",
            action="store_true",
            default=False,
            help="Run until all files processed (sleeping if needed), else process one file and quit.",
        )
        ap.add_argument(
            "--random-sample",
            type=float,
            default=None,
            metavar="PERCENT",
            help="Percentage of stories to queue for testing (default: all)",
        )

        # _could_ be mutually exclusive with --max-count and --random-sample
        # instead, warnings output below
        ap.add_argument(
            "--sample-size",
            type=int,
            default=None,
            metavar="N",
            help=f"Implies --max-stories N --random-sample {self.SAMPLE_PERCENT}",
        )

        self.input_group = ap.add_argument_group()
        assert self.input_group is not None
        self.input_group.add_argument("input_files", nargs="*", default=None)

    def process_args(self) -> None:
        super().process_args()
        args = self.args
        assert args

        if args.sample_size is not None:
            # could make options mutually exclusive, but would rather just complain:
            if args.max_stories is not None:
                logger.warning(
                    "--sample-size %s with --max-stories %s",
                    args.sample_size,
                    args.max_stories,
                )
            if args.max_stories is not None:
                logger.warning(
                    "--sample-size with --random-sample %s",
                    args.random_sample,
                )
            args.max_stories = args.sample_size
            args.random_sample = self.SAMPLE_PERCENT

    def send_story(self, story: BaseStory, check_html: bool = False) -> None:
        assert self.args

        url = story.http_metadata().final_url or story.rss_entry().link or ""
        if not self.check_story_url(url):
            return  # logged and counted

        if check_html:
            html = story.raw_html().html or b""
            if not self.check_story_length(html, url):
                return  # logged and counted

        level = logging.INFO
        count = True
        if (
            self.args.random_sample is not None
            and random.random() * 100 > self.args.random_sample
        ):
            # here for randomly selecting URLs for testing
            status = "dropped"  # should not be seen in production!!!
            level = logging.DEBUG
            count = False  # don't count against limit!
        elif self.args.dry_run:
            status = "parsed"
        else:
            if self.sender is None:
                self.sender = self.story_sender()
            self.sender.send_story(story)
            status = "success"

        self.incr_stories(status, url, log_level=level)

        if not count:
            return

        self.queued_stories += 1
        if (
            self.args.max_stories is not None
            and self.queued_stories >= self.args.max_stories
        ):
            logger.info("%s %s stories; quitting", status, self.queued_stories)
            sys.exit(0)

    def process_file(self, fname: str, fobj: BinaryIO) -> None:
        """
        Override, calling "self.queue_story" for each Story
        NOTE! fobj is a binary file!
        Wrap with TextIOWrapper for reading strings
        """
        raise NotImplementedError("process_file not overridden")

    def check_output_queues(self) -> None:
        """
        snooze while output queue(s) have enough work;
        if in "try one and quit" (crontab) mode, just quit.
        """

        assert self.args
        max_queue = self.args.max_queue_len

        # get list of queues fed from this app's output exchange
        admin = self.admin_api()
        defns = admin.get_definitions()
        output_exchange = self.output_exchange_name
        queue_names = set(
            [
                binding["destination"]
                for binding in defns["bindings"]
                if binding["source"] == output_exchange
            ]
        )

        while True:
            # also wanted/used by scripts.rabbitmq-stats:
            queues = admin._api_get("/api/queues")
            for q in queues:
                name = q["name"]
                if name in queue_names:
                    ready = q["messages_ready"]
                    logger.debug("%s: ready %d", name, ready)
                    if ready > max_queue:
                        break
            else:
                # here when all queues short enough
                return

            if self.args.loop:
                logger.debug("sleeping until output queue(s) shorter")
                time.sleep(60)
            else:
                logger.info("queue(s) full enough: quitting")
                sys.exit(0)

    def s3_client(self) -> S3Client:
        """
        return an S3 client object.
        """
        if not self.s3_client_object:
            # NOTE! None values should default to using ~/.aws/credentials
            # for command line debug/test.
            for app in [self.AWS_PREFIX.upper(), "QUEUER"]:
                region = os.environ.get(f"{app}_S3_REGION")
                access_key_id = os.environ.get(f"{app}_S3_ACCESS_KEY_ID")
                secret_access_key = os.environ.get(f"{app}_S3_SECRET_ACCESS_KEY")
                if region and access_key_id and secret_access_key:
                    break
            self.s3_client_object = boto3.client(
                "s3",
                region_name=region,
                aws_access_key_id=access_key_id,
                aws_secret_access_key=secret_access_key,
            )
        return self.s3_client_object

    def split_s3_url(self, objname: str) -> List[str]:
        """
        assumes starts with s3://
        returns [bucket, key]
        """
        path = objname[5:]
        if "/" in path:
            return path.split("/", 1)
        return [path, ""]

    def open_file(self, fname: str) -> BinaryIO:
        """
        take local file path or a URL
        return BinaryIO file object (optionally decompressed)
        """
        if os.path.isfile(fname):
            if self.HANDLE_GZIP and fname.endswith(".gz"):
                # read/uncompress local gzip'ed file
                gzio = gzip.GzipFile(fname, "rb")
                assert isinstance(gzio, io.IOBase)
                return cast(BinaryIO, gzio)
            # read local file:
            return open(fname, "rb")

        if fname.startswith("http:") or fname.startswith("https:"):
            resp = requests.get(fname, stream=True, timeout=60)
            if not resp or resp.status_code != 200:
                raise AppException(str(resp))
            # (resp.raw is urllib3.response.HTTPResponse,
            # which is a subclass of io.IOBase)
            assert isinstance(resp.raw, io.IOBase)
            fobj = cast(BinaryIO, resp.raw)
        elif fname.startswith("s3://"):  # XXX handle any "blobstore" url?
            bucket, objname = self.split_s3_url(fname)
            s3 = self.s3_client()
            # anonymous temp file: maybe cache in named file?
            tmpf = tempfile.TemporaryFile()
            s3.download_fileobj(bucket, objname, tmpf)
            tmpf.seek(0)  # rewind
            fobj = cast(BinaryIO, tmpf)
        else:
            raise AppException("file not found or unknown URL")

        # uncompress on the fly?
        if self.HANDLE_GZIP and fname.endswith(".gz"):
            logger.debug("zcat ")
            gzio = gzip.GzipFile(filename=fname, mode="rb", fileobj=fobj)
            return cast(BinaryIO, gzio)

        return fobj

    def maybe_process_files(self, fname: str) -> None:
        """
        supports simple prefix matching for s3 URLs
        _COULD_ do full wildcarding by fetching all and applying glob.glob.
        (would be more efficient to do prefix (as below), stopping at
        first wildcard character
        """
        if os.path.isdir(fname):  # local directory
            logger.debug("walking directory tree %s", fname)
            for root, dirs, files in os.walk(fname, topdown=False):
                for name in files:
                    self.maybe_process_file(os.path.join(root, name))
        elif os.path.isfile(fname):  # local file
            self.maybe_process_file(fname)
        elif fname.startswith("s3://"):  # XXX handle any blobstore URL?
            self.maybe_process_s3_prefix(fname)
        else:
            logger.warning("bad path or url: %s", fname)

    def maybe_process_s3_prefix(self, url: str) -> None:
        # enumerate all matching objects
        bucket, prefix = self.split_s3_url(url)
        s3 = self.s3_client()
        marker = ""
        while True:
            res = s3.list_objects(Bucket=bucket, Prefix=prefix, Marker=marker)
            for item in res["Contents"]:
                key = item["Key"]
                logger.debug("key %s", key)
                self.maybe_process_file(f"s3://{bucket}/{key}")
            if not res["IsTruncated"]:
                break
            marker = key  # see https://github.com/boto/boto3/issues/470
            logger.debug("object list truncated; next marker: %s", marker)
            if not marker:
                break

    def maybe_process_file(self, fname: str) -> None:
        args = self.args
        assert args

        # wait until queue(s) low enough, or quit:
        self.check_output_queues()

        def incr_files(status: str) -> None:
            self.incr("files", labels=[("status", status)])

        # no tracking if ignoring tracker or sampling/testing
        testing = (
            args.force or args.max_stories is not None or args.random_sample is not None
        )
        try:
            tracker = get_tracker(self.process_name, fname, testing)
            try:
                with tracker:
                    f = self.open_file(fname)
                    logger.info("process_file %s", fname)
                    self.process_file(fname, f)
                incr_files("success")
                if self.args and not self.args.loop:
                    # you CAN eat just one!
                    sys.exit(0)
            except Exception as e:  # YIKES (reraised)
                logger.error("%s failed: %r", fname, e)
                incr_files("failed")
                raise
        except TrackerException as exc:
            # here if file state other than "NOT_STARTED"
            logger.info("%s: %r", fname, exc)
            return

    def main_loop(self) -> None:
        assert self.args

        if not self.args.input_files:
            logger.error("no inputs!")
            sys.exit(1)

        # command line items may include S3 wildcards
        for item in self.args.input_files:
            self.maybe_process_files(item)
