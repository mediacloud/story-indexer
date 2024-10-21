"""
Media Cloud Archiver Worker
"""

import logging
import os
import socket
import time

import indexer.blobstore
from indexer.app import run
from indexer.path import DATAROOT
from indexer.story import BaseStory
from indexer.story_archive_writer import (
    ArchiveStoryError,
    FileobjError,
    StoryArchiveWriter,
)
from indexer.storyapp import BatchStoryWorker, StorySender
from indexer.worker import QuarantineException

logger = logging.getLogger("indexer.workers.archiver")

FQDN = socket.getfqdn()  # most likely internal or container!
HOSTNAME = socket.gethostname()  # for filenames


class Archiver(BatchStoryWorker):
    def __init__(self, process_name: str, descr: str):
        super().__init__(process_name, descr)

        self.archive: StoryArchiveWriter | None = None
        self.archive_prefix = os.environ.get("ARCHIVER_PREFIX", "mc")
        self.blobstores: list[indexer.blobstore.BlobStore] = []

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

        # returns list of 0 or more BlobStore provider objects
        self.blobstores = indexer.blobstore.blobstores("ARCHIVER")
        logger.info(
            "blobstores configured: %s",
            ", ".join(bs.PROVIDER for bs in self.blobstores),
        )

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
                rw=True,
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

            if self.stories == 0:
                try:
                    os.unlink(local_path)
                    logger.info("removed empty %s", local_path)
                except OSError as e:
                    logger.warning("unlink empty %s failed: %r", local_path, e)
                status = "empty"
            elif self.blobstores:
                # S3 rate limits requests to
                #  3500 PUTs/s and 5500 GETs/s per prefix.
                #  Varying the prefix allows faster retrieval.
                prefix = time.strftime("%Y/%m/%d/", time.gmtime(timestamp))
                remote_path = prefix + name
                errors = 0

                # NOTE! If upload fails for any single blobstore, the
                # file will be left in the work (archiver) directory
                # without indication of which upload fails.  We've
                # only used multiple blobstores when switching from S3
                # to B2, but if multiple stores ever become the norm,
                # consider having multiple archiver input queues
                # fed by a fanout exchange, ie; have
                # two "add_consumer" calls in the "add_worker"
                # line for importer in pipeline.py

                for bs in self.blobstores:
                    try:
                        fileobj = self.archive.fileobj()  # inside try
                        fileobj.seek(0, 0)  # rewind!
                        t0 = time.monotonic()
                        bs.upload_fileobj(fileobj, remote_path)
                        sec = time.monotonic() - t0
                        self.timing(
                            "upload",
                            sec * 1000,
                            labels=[("store", bs.PROVIDER)],
                        )
                        # could have upload_speed size/sec!
                        logger.info(
                            "uploaded %s to %s %s %d b %.3f s",
                            local_path,
                            bs.PROVIDER,
                            remote_path,
                            size,
                            sec,
                        )
                    except tuple(bs.EXCEPTIONS + [FileobjError]) as e:
                        logger.error(
                            "archive %s upload to %s failed: %r", name, bs.PROVIDER, e
                        )
                        errors += 1
                self.archive.close()
                if errors == 0:
                    status = "uploaded"
                    self.maybe_unlink_local(local_path)
                else:
                    status = "noupload"
            else:  # no blobstores
                status = "nostore"
            del self.archive
            self.archive = None
        else:
            logger.info("no archive?")  # want "notice" level!
            status = "noarch"

        self.incr("batches", labels=[("status", status)])
        self.stories = 0


if __name__ == "__main__":
    run(Archiver, "archiver", "story archiver")
