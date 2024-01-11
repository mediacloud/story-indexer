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

import indexer.blobstore
from indexer.app import run
from indexer.path import DATAROOT
from indexer.story import BaseStory
from indexer.story_archive_writer import ArchiveStoryError, StoryArchiveWriter
from indexer.storyapp import BatchStoryWorker, StorySender
from indexer.worker import QuarantineException

logger = logging.getLogger("indexer.workers.archiver")

FQDN = socket.getfqdn()  # most likely internal or container!
HOSTNAME = socket.gethostname()  # for filenames


class Archiver(BatchStoryWorker):
    def __init__(self, process_name: str, descr: str):
        super().__init__(process_name, descr)

        self.archive: Optional[StoryArchiveWriter] = None
        self.archive_prefix = os.environ.get("ARCHIVER_PREFIX", "mc")
        self.blobstore: Optional[indexer.blobstore.BlobStore] = None

        self.stories = 0  # stories written to current archive
        self.archives = 0  # number of archives written

        # default to Docker worker volume so files persist if not uploaded
        self.work_dir = os.environ.get("ARCHIVER_WORK_DIR", DATAROOT() + "archiver")

    def process_args(self) -> None:
        super().process_args()

        # here so logging configured
        if not os.path.isdir(self.work_dir):
            os.makedirs(self.work_dir)
            logger.info("created work directory %s", self.work_dir)

        self.blobstore = indexer.blobstore.blobstore("ARCHIVER")

    def process_story(self, sender: StorySender, story: BaseStory) -> None:
        """
        Process story; do any heavy lifting here, or at least validate!!!
        Raise QuarantineException to quarantine this story,
        any other exception will cause this story to be retried.
        """

        if not self.archive:
            self.archives += 1
            self.archive = StoryArchiveWriter(
                prefix=self.archive_prefix,
                hostname=HOSTNAME,
                fqdn=FQDN,
                serial=self.archives,
                work_dir=self.work_dir,
            )
            self.stories = 0

        try:
            self.archive.write_story(story)
            self.stories += 1
        except ArchiveStoryError as e:
            logger.info("write_story: %r", e)
            self.incr("stories.{e}")
            raise QuarantineException(repr(e))  # for now

    def maybe_unlink_local(self, path: str) -> None:
        # quick-and-dirty change to prevent removal of archives
        # any non-empty value causes removal:
        if not os.getenv("ARCHIVER_REMOVE_LOCAL", ""):
            # XXX maybe rename to prefixed directory structure?
            return

        try:
            os.unlink(path)
            logger.info("removed %s", path)
        except OSError as e:
            logger.warning("unlink %s failed: %r", path, e)

    def end_of_batch(self) -> None:
        """
        Here to process collected work.
        Any exception will cause all stories to be retried.
        """
        logger.info("end of batch: %d stories", self.stories)
        # could report count of stories as a "timer" (not just for milliseconds!)
        if self.archive:
            self.archive.finish()
            name = self.archive.filename
            local_path = self.archive.full_path
            size = self.archive.size
            timestamp = self.archive.timestamp

            logger.info(
                "wrote %d stories to %s (%s bytes)", self.stories, local_path, size
            )
            del self.archive
            self.archive = None

            if self.stories == 0:
                try:
                    os.unlink(local_path)
                    logger.info("removed empty %s", local_path)
                except OSError as e:
                    logger.warning("unlink empty %s failed: %r", local_path, e)
                status = "empty"
            elif self.blobstore:
                # S3 rate limits requests to
                #  3500 PUTs/s and 5500 GETs/s per prefix.
                #  Varying the prefix allows faster retrieval.
                prefix = time.strftime("%Y/%m/%d/", time.gmtime(timestamp))
                remote_path = prefix + name

                try:
                    self.blobstore.store_from_local_file(local_path, remote_path)
                    logger.info(
                        "uploaded %s to %s %s",
                        local_path,
                        self.blobstore.PROVIDER,
                        remote_path,
                    )
                    self.maybe_unlink_local(local_path)
                    status = "uploaded"
                except tuple(self.blobstore.EXCEPTIONS) as e:
                    logger.error("archive %s upload failed: %r", name, e)  # exc_info?
                    status = "noupload"
            else:
                status = "nostore"
        else:
            logger.info("no archive?")  # want "notice" level!
            status = "noarch"

        self.incr("batches", labels=[("status", status)])
        self.stories = 0


if __name__ == "__main__":
    run(Archiver, "archiver", "story archiver")
