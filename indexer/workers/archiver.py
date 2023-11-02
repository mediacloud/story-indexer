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
        self.stories = 0
        self.archives = 0

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
            name, path = self.archive.finish()
            logger.info("wrote %d stories to %s", self.stories, path)
            del self.archive
            self.archive = None

            # report stories as a "timer" (get statistics)
            if self.stories == 0:
                logger.debug("no stories: removing %s", path)
                try:
                    os.unlink(path)
                except (FileNotFoundError, IsADirectoryError, PermissionError):
                    pass
                status = "empty"
            else:
                logger.info("UPLOAD %s", path)
                status = "uploaded"
                # XXX upload to S3????
        else:
            logger.info("no archive?")  # want "notice" level!
            status = "noarch"

        self.incr("batches", labels=[("status", status)])
        self.stories = 0


if __name__ == "__main__":
    run(Archiver, "archiver", "story archiver")
