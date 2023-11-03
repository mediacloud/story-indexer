"""
Media Cloud Archiver Worker
"""

import argparse
import logging
import os
import socket
import time
from enum import Enum
from io import BytesIO, RawIOBase
from typing import List, Optional

import boto3

from indexer.story import BaseStory
from indexer.story_archive_writer import ArchiveStoryError, StoryArchiveWriter
from indexer.worker import BatchStoryWorker, QuarantineException, StorySender, run

logger = logging.getLogger("indexer.workers.archiver")

FQDN = socket.getfqdn()  # most likely internal or container!
HOSTNAME = socket.gethostname()  # for filenames


class Archiver(BatchStoryWorker):
    def __init__(self, process_name: str, descr: str):
        super().__init__(process_name, descr)
        self.archive: Optional[StoryArchiveWriter] = None
        self.stories = 0  # stories written to current archive
        self.archives = 0  # number of archives written

        self.s3_region = os.environ.get("ARCHIVER_S3_REGION", None)
        self.s3_bucket = os.environ.get("ARCHIVER_S3_BUCKET", None)
        self.s3_access_key_id = os.environ.get("ARCHIVER_S3_ACCESS_KEY_ID", None)
        self.s3_secret_access_key = os.environ.get(
            "ARCHIVER_S3_SECRET_ACCESS_KEY", None
        )

    def process_story(self, sender: StorySender, story: BaseStory) -> None:
        """
        Process story; do any heavy lifting here, or at least validate!!!
        Raise QuarantineException to quarantine this story,
        any other exception will cause this story to be retried.
        """

        if not self.archive:
            realm = os.getenv("STATSD_REALM", "")
            if realm == "prod":
                prefix = "mc"
            elif realm:
                prefix = realm  # staging or username
            else:
                prefix = "unk"

            self.archives += 1
            self.archive = StoryArchiveWriter(
                prefix=prefix,
                hostname=HOSTNAME,
                fqdn=FQDN,
                serial=self.archives,
            )
            self.stories = 0

        try:
            self.archive.write_story(story)
            self.stories += 1
        except ArchiveStoryError as e:
            logger.info("caught %r", e)
            self.incr("stories.{e}")
            raise QuarantineException(repr(e))  # for now

    def end_of_batch(self) -> None:
        """
        Here to process collected work.
        Any exception will cause all stories to be retried.
        """
        logger.info("end of batch: %d stories", self.stories)
        if self.archive:
            name, path, size, ts = self.archive.finish()
            logger.info("wrote %d stories to %s (%s bytes)", self.stories, path, size)
            del self.archive
            self.archive = None

            # report stories as a "timer" (get statistics)
            if self.stories == 0:
                try:
                    os.unlink(path)
                    logger.info("removinged empty %s", path)
                except OSError as e:
                    logger.warning("unlink empty %s failed: %r", path, e)
                status = "empty"
            else:
                # S3 rate limits requests to
                #  3500 PUTs/s and 5500 GETs/s per prefix.
                #  So varying the prefix allows faster retrieval.
                prefix = time.strftime("%Y/%m/%d/", time.gmtime(ts))
                s3_key = prefix + name

                if (
                    self.s3_region
                    and self.s3_access_key_id
                    and self.s3_secret_access_key
                    and self.s3_bucket
                ):
                    # NOTE! Not under try: if fails will be retried!!
                    s3 = boto3.client(
                        "s3",
                        region_name=self.s3_region,
                        aws_access_key_id=self.s3_access_key_id,
                        aws_secret_access_key=self.s3_secret_access_key,
                    )
                    s3.upload_file(path, self.s3_bucket, s3_key)
                    logger.info("uploaded %s to %s:%s", path, self.s3_bucket, s3_key)
                    os.unlink(path)
                    status = "uploaded"
                else:
                    logger.error("NO S3 CONFIGURATION!!!")
                    # not a big deal for development;
                    if self.s3_bucket == "NO_ARCHIVE":
                        status = "noarchive"
                    else:
                        # force retry to avoid loss of stories
                        raise Exception("no s3 config")
        else:
            logger.info("no archive?")  # want "notice" level!
            status = "noarch"

        self.incr("batches", labels=[("status", status)])
        self.stories = 0


if __name__ == "__main__":
    run(Archiver, "archiver", "story archiver")
