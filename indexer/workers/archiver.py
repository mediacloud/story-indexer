"""
Media Cloud Archiver
"""

import argparse
import logging
import os
import time
from io import BytesIO
from typing import List

from warcio.statusandheaders import StatusAndHeaders
from warcio.warcwriter import WARCWriter

from indexer.story import BaseStory
from indexer.worker import BatchStoryWorker, QuarantineException, StorySender, run

logger = logging.getLogger("indexer.workers.archiver")


class Archiver(BatchStoryWorker):
    def __init__(self, process_name: str, descr: str):
        super().__init__(process_name, descr)
        self.work: List[bytes] = []  # list of pre-processed work

        # temporarily create files in data volume for visibility:
        self.work_dir = time.strftime("/app/data/%Y-%m-%d-%H-%M-%S", time.gmtime())
        if not os.path.isdir(self.work_dir):
            os.mkdir(self.work_dir)

        self.file_number = 1

    def process_story(self, sender: StorySender, story: BaseStory) -> None:
        """
        Process story; do any heavy lifting here, or at least validate!!!
        Raise QuarantineException to quarantine this story,
        any other exception will cause this story to be retried.
        """

        re = story.rss_entry()
        hmd = story.http_metadata()
        cmd = story.content_metadata()
        rhtml = story.raw_html()

        # synthesize (forge) from available info
        # almost certainly needs work (show redirects better?)
        # taking the position that "failure is not an option"

        url = hmd.final_url or cmd.url or cmd.original_url or re.link
        html = rhtml.html
        if url and html:
            logger.info("process_story %s", url)

            rcode = hmd.response_code
            if rcode is not None and rcode != 200:
                resp = f"{rcode:03d} HUH?"
            else:
                resp = "200 OK"

            headers_list = []
            # could call rhtml.guess_encoding, but so can reader
            if encoding := hmd.encoding or rhtml.encoding:
                # Danger! a peek inside warcio looks like
                # no headers causes create_warc_record to do different stuff!
                headers_list.append(("Content-Type", f"text/html; charset={encoding}"))

            http_headers = StatusAndHeaders(resp, headers_list, protocol="HTTP/1.0")

            # started from https://pypi.org/project/warcio/#description
            # "Manual/Advanced WARC Writing"

            # write directly to a ZipFile (disk or memory)??
            fname = f"{self.work_dir}/{self.file_number:05d}.warc.gz"
            with open(fname, "wb") as output:
                writer = WARCWriter(output, gzip=True)
                record = writer.create_warc_record(
                    url, "response", payload=BytesIO(html), http_headers=http_headers
                )
                writer.write_record(record)
            self.file_number += 1
        else:
            # counters!! separate for each??
            raise QuarantineException("no url or html")

    def end_of_batch(self) -> None:
        """
        Here to process collected work.
        Any exception will cause all stories to be retried.
        """
        logger.info("end of batch: %d stories", len(self.work))
        self.work = []


if __name__ == "__main__":
    run(Archiver, "archiver", "story archiver")
