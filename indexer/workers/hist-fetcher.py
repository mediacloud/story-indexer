"""
Read CSV of articles from legacy system, using HTML stored
on Amazon S3, indexed by downloads_id, and queue for processing

TODO?
Toss stories w/ domain name ending in member of NON_NEWS_DOMAINS
Take s3 URL for input file
Drop empty FILENAME.done turn on S3 for completed files?
Take s3 URL for file w/ list of input files??
"""

import argparse
import csv
import gzip
import io
import logging
import os
import sys
from typing import Any, Dict, List, Optional, Tuple, TypedDict

import boto3
from mcmetadata.urls import NON_NEWS_DOMAINS

from indexer.story import BaseStory, StoryFactory
from indexer.worker import StoryProducer, run

logger = logging.getLogger(__name__)

Story = StoryFactory()

# NOTE! Two database epochs (B & D) have overlapping download ids:
# DB B: 2021-09-15 00:00:00.921164 thru 2021-11-11 00:02:17.402188?
# DB D: 2021-12-26 10:48:42.490858 thru 2022-01-25 00:00:00.072873?

# db-b/stories_2021_09_15.csv:
# 2021-09-15 16:36:53.505277,2043951575,18710,3211617068,59843,https://www.businessinsider.com/eff-google-daily-stormer-2017-8#comments

# attrs from boto (not necessarily story downloads):
# downloads/3211617604 sbRmtvTcbmDH.dxWNcIBsmRz.ffbbosG 18620 2021-09-15T20:51:01
# downloads/3211617605 dSQTc9dyyVj34GP7zMI0GfOFmEkaQE_K 13761 2021-09-15T20:50:59
# downloads/3211617605 fKUnm9Sbr8Gt33FaY0gUoKxaq5J3kY1l 36 2021-12-27T05:11:47
# ....
# downloads/3257240456 N5Bn9xkkeGgXI1BSSzl71hRi9eiHCMo8 5499 2021-10-16T08:53:20
# downloads/3257240456 Vp_Qfu7Yo1QkZqWA4RRYu83h0PFoFLOz 8853 2022-01-25T15:47:27
# downloads/3257240457 N4tYO8GEnt6_px8SHE9VYc5B5N9Yp_hN 36 2021-10-16T08:53:19

OVERLAP_START = 3211617605  # lowest dl_id w/ multiple versions?
OVERLAP_END = 3257240456  # hightest dl_lid w/ multiple versions?

DB_B_END = "2021-11-12"  # latest possible data in DB B
DB_D_START = "2021-12-26"  # earliest possible date in DB D

DOWNLOADS_BUCKET = "mediacloud-downloads-backup"
DOWNLOADS_PREFIX = "downloads/"

# XXX get this from common config?
MAX_HTML_SIZE = 10000000  # 10MB


class HistFetcher(StoryProducer):
    AUTO_CONNECT: bool = False

    def define_options(self, ap: argparse.ArgumentParser) -> None:
        super().define_options(ap)
        ap.add_argument("csv", help="CSV file of stories")
        ap.add_argument(
            "--count", type=int, default=4294967296, help="count of stories to queue"
        )

    def main_loop(self) -> None:
        assert self.args

        count = self.args.count

        # XXX take keys from environment
        s3 = boto3.resource("s3")
        bucket = s3.Bucket(DOWNLOADS_BUCKET)

        self.qconnect()
        sender = self.story_sender()
        self.start_pika_thread()

        # expect local file for now
        with open(self.args.csv) as f:
            for row in csv.DictReader(f):
                # typ columns: collect_date,stories_id,media_id,downloads_id,feeds_id,[language,]url
                logger.info("%r", row)

                url = row.get("url", None)
                if not url:
                    logger.error("no url: %r", row)
                    # XXX counter?
                    continue

                try:
                    dlid = int(row.get("downloads_id") or "")
                except (KeyError, ValueError):
                    logger.error("bad downloads_id: %r", row)
                    # XXX counter?
                    continue

                if dlid >= OVERLAP_START and dlid <= OVERLAP_END:
                    logger.error("download id %d in overlap region", dlid)
                    # XXX need to retrieve available versions and pick!!
                    continue

                # 2023 era legacy system always wrote UTF-8?
                # need to revisit for older archives!!!!
                # XXX test if S3 data decodes w/o errors!!!!!
                encoding = "utf-8"

                story = Story()
                with story.rss_entry() as rss:
                    rss.link = url  # only used for logging

                with story.http_metadata() as hmd:
                    hmd.final_url = url

                with io.BytesIO() as bio:
                    s3path = DOWNLOADS_PREFIX + str(dlid)
                    bucket.download_fileobj(s3path, bio)
                    html = gzip.decompress(bio.getbuffer())

                logger.info("%s: %d bytes", url, len(html))
                # XXX check length
                with story.raw_html() as raw:
                    raw.encoding = encoding
                    raw.html = html

                lang = row.get("language", None)
                if lang:
                    with story.content_metadata() as cmd:
                        cmd.language = cmd.full_language = lang

                sender.send_story(story)
                # counter?

                count -= 1
                if count <= 0:
                    logger.info("reached max count")
                    break


if __name__ == "__main__":
    run(
        HistFetcher,
        "hist-fetcher",
        "Read CSV of historical stories, fetches HTML from S3 and queues",
    )
