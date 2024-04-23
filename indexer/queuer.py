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
import sys
import tempfile
from typing import TYPE_CHECKING, Any, BinaryIO, Generator, List, Optional, cast

import boto3
import requests

if TYPE_CHECKING:
    from mypy_boto3_s3.client import S3Client
else:
    S3Client = Any

from indexer.app import AppException
from indexer.storyapp import StoryProducer
from indexer.tracker import TrackerException, get_tracker

logger = logging.getLogger(__name__)


class Queuer(StoryProducer):
    AWS_PREFIX: str  # prefix for environment vars

    HANDLE_GZIP: bool  # True to intervene if .gz present

    def __init__(self, process_name: str, descr: str):
        super().__init__(process_name, descr)
        self.s3_client_object: Optional[S3Client] = None

    def define_options(self, ap: argparse.ArgumentParser) -> None:
        super().define_options(ap)

        ap.add_argument(
            "--test",
            action="store_true",
            default=False,
            help="Enumerate, but do not process files for testing",
        )
        ap.add_argument(
            "--cleanup",
            action="store_true",
            default=False,
            help="clean up old, incompletely processed files",
        )
        ap.add_argument("input_files", nargs="*", default=None)

    def process_file(self, fname: str, fobj: BinaryIO) -> None:
        """
        Override, calling "self.queue_story" for each Story
        NOTE! fobj is a binary file!
        Wrap with TextIOWrapper for reading strings
        """
        raise NotImplementedError("process_file not overridden")

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
        called from main:
        supports simple prefix matching for s3 URLs, local directories
        """
        if os.path.isdir(fname):  # local directory
            logger.debug("walking directory tree %s", fname)
            paths = []
            for root, dirs, files in os.walk(fname, topdown=False):
                for name in files:
                    # XXX filter based on file name ending?
                    paths.append(os.path.join(root, name))
            # process in reverse sorted/chronological order (back-fill):
            for path in sorted(paths, reverse=True):
                self.maybe_process_file(path)
        elif fname.startswith("s3://"):  # XXX handle any blobstore URL?
            # process in reverse sorted/chronological order (back-fill):
            for url in sorted(self.s3_prefix_matches(fname), reverse=True):
                self.maybe_process_file(url)
        else:  # local files, http, https
            self.maybe_process_file(fname)

    def s3_prefix_matches(self, url: str) -> Generator:
        """
        generator to enumerate all matching objects
        (push into blobstore?)
        """
        logger.debug("s3_prefix_matches: %s", url)
        bucket, prefix = self.split_s3_url(url)
        s3 = self.s3_client()
        marker = ""
        while True:
            res = s3.list_objects(Bucket=bucket, Prefix=prefix, Marker=marker)
            for item in res["Contents"]:
                key = item["Key"]
                # XXX filter based on ending?
                out = f"s3://{bucket}/{key}"
                logger.debug("match: %s", out)
                yield out
            if not res["IsTruncated"]:
                break
            marker = key  # see https://github.com/boto/boto3/issues/470
            logger.debug("object list truncated; next marker: %s", marker)
            if not marker:
                break

    def maybe_process_file(self, fname: str) -> None:
        """
        here with a single file (not a directory or prefix).
        checks if queue needs refilling, file already processed.
        """
        args = self.args
        assert args

        if args.test:
            logger.info("maybe_process_file %s", fname)
            return

        # wait until queue(s) low enough (if looping), or quit if full enough
        self.check_output_queues()

        queued_before = self.queued_stories

        def incr_files(status: str, exc: Optional[Exception] = None) -> None:
            self.incr("files", labels=[("status", status)])

            queued = self.queued_stories - queued_before
            if exc is None:
                logger.info("%s %s; %d stories", fname, status, queued)
            else:
                logger.info("%s %s: %r (%d stories)", fname, status, exc, queued)

        # no tracking if ignoring tracker or sampling/testing
        testing = (
            args.force
            or args.max_stories is not None
            or args.random_sample is not None
            or args.test
        )
        try:
            tracker = get_tracker(self.process_name, fname, testing, args.cleanup)
            with tracker:
                f = self.open_file(fname)
                logger.info("process_file %s", fname)
                with self.timer("process_file"):  # report elapsed time
                    self.process_file(fname, f)
                incr_files("success")
        except TrackerException as exc:
            # here if file not startable
            # or could not write to tracker database
            incr_files("skipped", exc)
        except Exception as exc:  # YIKES (reraised)
            incr_files("failed", exc)
            raise

    def main_loop(self) -> None:
        assert self.args

        if not self.args.input_files:
            logger.error("no inputs!")
            sys.exit(1)

        # command line items may include S3 wildcards, local directories
        for item in self.args.input_files:
            self.maybe_process_files(item)
        logger.info("end of input files: %d stories", self.queued_stories)


if __name__ == "__main__":
    # here via "python -m indexer.queuer" for testing options, path expansion
    from indexer.app import run

    class TestQueuer(Queuer):
        AWS_PREFIX = "FOO"
        HANDLE_GZIP = True

        def process_file(self, fname: str, fobj: BinaryIO) -> None:
            print("process_file", fname, fobj)

    run(TestQueuer, "queue-rss", "parse and queue rss-fetcher RSS entries")
