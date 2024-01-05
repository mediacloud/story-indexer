"""
Queuer: a StoryProducer class for reading (possibly remote) input
files, processing them, and queuing Stories for "fetching"

Supports http(s) and S3 file sources, with limited S3 "globbing"

Uses "indexer.tracker" to keep track of already processed files.

Made into a class because several input paths need the same logic:
1. Historical Ingest: reading CSV files of db dumps, fetching HTTP from S3
2. Queue based fetcher: reading rss-fetcher generated RSS files
3. Reading and replaying WARC archive files

By default will loop for all files on command line (checking queue
lengths and sleeping).  Alternative "one-file" mode finds at most one
file to process and exits, which is (more) suitable for use from a
crontab.
"""

import argparse
import gzip
import io
import logging
import os
import sys
import tempfile
import time
from enum import Enum
from typing import Any, List, Optional, Tuple

import boto3
import botocore
import requests
import urllib3
from mypy_boto3_s3.client import S3Client

from indexer.app import AppException
from indexer.tracker import TrackerException, get_tracker
from indexer.worker import BaseStory, StoryProducer, StorySender

logger = logging.getLogger(__name__)


class Queuer(StoryProducer):
    MAX_QUEUE = 100000

    AWS_PREFIX: str  # prefix for environment vars

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
            "--max-queue",
            type=int,
            default=self.MAX_QUEUE,
            help=f"Maximum queue length at which to send a new batch (default: {self.MAX_QUEUE})",
        )
        ap.add_argument(
            "--max-stories",
            type=int,
            default=None,
            help="Number of stories to queue. Default (None) is 'all of them'",
        )
        ap.add_argument(
            "--one-file",
            "-1",  # digit one
            action="store_true",
            default=False,
            help="Try at most one file and quit; for crontab use",
        )

        self.input_group = ap.add_argument_group()
        assert self.input_group is not None
        self.input_group.add_argument("input_files", nargs="*", default=None)

    def queue_story(self, story: BaseStory) -> None:
        assert self.args
        if self.args.dry_run:
            return
        if self.sender is None:
            self.sender = self.story_sender()
        self.sender.send_story(story)
        self.queued_stories += 1
        self.incr("stories")
        if (
            self.args.max_stories is not None
            and self.queued_stories >= self.args.max_stories
        ):
            logger.info("queued %s stories; quitting", self.queued_stories)
            sys.exit(0)

    def process_file(self, fname: str, fobj: io.IOBase) -> None:
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
        max_queue = self.args.max_queue

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

            if self.args and self.args.one_file:
                logger.info("queue(s) full enough: quitting")
                sys.exit(0)
            logger.debug("sleeping until output queue(s) shorter")
            time.sleep(60)

    def s3_client(self) -> S3Client:
        if not self.s3_client_object:
            app = self.AWS_PREFIX.upper()
            # NOTE! None values should default to using ~/.aws/credentials
            # for command line debug/test.
            region = os.environ.get(f"{app}_S3_REGION")
            access_key_id = os.environ.get(f"{app}_S3_ACCESS_KEY_ID")
            secret_access_key = os.environ.get(f"{app}_S3_SECRET_ACCESS_KEY")
            self.s3_client_object = boto3.client(
                "s3",
                region_name=region,
                aws_access_key_id=access_key_id,
                aws_secret_access_key=secret_access_key,
            )
        return self.s3_client_object

    def split_s3_url(self, objname: str) -> List[str]:
        # assumes s3:// prefix: returns at most two items
        return objname[5:].split("/", 1)

    def open_file(self, fname: str) -> io.IOBase:
        if fname.startswith("http:") or fname.startswith("https:"):
            resp = requests.get(fname)
            if resp and resp.status_code == 200:
                assert isinstance(resp.raw, urllib3.response.HTTPResponse)
                return resp.raw
            raise AppException(str(resp))

        if fname.startswith("s3://"):
            bucket, objname = self.split_s3_url(fname)
            s3 = self.s3_client()
            # anonymous temp file: maybe cache in named file?
            tmpf = tempfile.TemporaryFile()
            s3.download_fileobj(bucket, objname, tmpf)
            tmpf.seek(0)  # rewind
            return tmpf

        return open(fname, "rb")

    def maybe_process_files(self, fname: str) -> None:
        """
        supports simple prefix matching for s3 URLs
        """
        if fname.startswith("s3://") and fname.endswith("*"):
            bucket, prefix = self.split_s3_url(fname[:-1])
            s3 = self.s3_client()

            marker = ""  # try handling pagination
            while True:
                res = s3.list_objects(Bucket=bucket, Prefix=prefix, Marker=marker)
                for item in res["Contents"]:
                    key = item["Key"]
                    logger.info("key %s", key)
                    self.maybe_process_file(f"s3://{bucket}/{key}")
                if not res["IsTruncated"]:
                    break
                marker = key  # see https://github.com/boto/boto3/issues/470
                logger.info("object list truncated; next marker: %s", marker)
                if not marker:
                    break
        else:
            self.maybe_process_file(fname)

    def maybe_process_file(self, fname: str) -> None:
        # wait until queue(s) low enough, or quit:
        self.check_output_queues()

        def incr_files(status: str) -> None:
            self.incr("files", labels=[("status", status)])

        try:
            tracker = get_tracker(self.process_name, fname)
            try:
                with tracker:
                    f = self.open_file(fname)
                    logger.info("process_file %s", fname)
                    self.process_file(fname, f)
                incr_files("success")
                if self.args and self.args.one_file:
                    sys.exit(0)
            except RuntimeError as e:  # YIKES!!!
                logger.error("%s failed: %r", fname, e)
                incr_files("failed")
                # XXX re-raise???
        except TrackerException as exc:
            # here if file state other than "NOT_STARTED"
            logger.info("%s: %r", fname, exc)
            return

    def main_loop(self) -> None:
        assert self.args
        # command line items may include S3 wildcards
        for item in self.args.input_files:
            self.maybe_process_files(item)
