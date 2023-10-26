"""
Media Cloud Archiver
"""

import argparse
import logging
from typing import List

from indexer.story import BaseStory
from indexer.worker import BatchStoryWorker, QuarantineException, StorySender, run

logger = logging.getLogger("indexer.workers.archiver")


class Archiver(BatchStoryWorker):
    def __init__(self, process_name: str, descr: str):
        super().__init__(process_name, descr)
        self.work: List[bytes] = []  # list of pre-processed work

    def process_story(self, sender: StorySender, story: BaseStory) -> None:
        """
        Process story; do any heavy lifting here, or at least validate!!!
        Raise QuarantineException to quarantine this story,
        any other exception will cause this story to be retried.
        """

        content_metadata = story.content_metadata()
        if content_metadata and (url := content_metadata.url):
            assert isinstance(url, str)
            logger.info("process_story %s", url)
            self.work.append(story.dump())  # pre-processed data
        else:
            raise QuarantineException("no content_metadata or url")

    def end_of_batch(self) -> None:
        """
        Here to process collected work.
        Any exception will cause all stories to be retried.
        """
        logger.info("end of batch: %d stories", len(self.work))
        self.work = []


if __name__ == "__main__":
    run(Archiver, "archiver", "story archiver")
