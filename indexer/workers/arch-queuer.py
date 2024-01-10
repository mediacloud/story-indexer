"""
Worker to read archive files, and queue.
Can read from S3, http, and local files.
Keeps track of files already queued.
"""

import argparse
import csv
import io
import logging
import os
import sys
from typing import BinaryIO

from indexer.app import run
from indexer.queuer import Queuer
from indexer.story_archive_writer import StoryArchiveReader

logger = logging.getLogger(__name__)


class ArchiveQueuer(Queuer):
    AWS_PREFIX = "ARCHIVER"  # same as archiver.py
    HANDLE_GZIP = False  # handled by warcio

    def process_file(self, fname: str, fobj: BinaryIO) -> None:
        reader = StoryArchiveReader(fobj)
        for story in reader.read_stories():
            self.send_story(story, check_html=True)


if __name__ == "__main__":
    run(ArchiveQueuer, "arch-queuer", "Read Archive files and queue")
