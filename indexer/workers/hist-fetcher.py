"""
Fetch stories archived on S3 by legacy system
using Stories created from CSV by hist-queuer
(reading CSV files from S3)

Separate from hist-queuer because S3 latency prevents a single process
from getting anything near the (single-prefix) fetch limit of 4500
objects/second, AND to make it so that crashes/interruptions in the
fetch keep track of what has already been handled.
"""

import argparse
import csv
import gzip
import io
import logging
import os
import sys
from typing import Any, Dict, Optional

import boto3

from indexer.app import run
from indexer.story import BaseStory
from indexer.storyapp import StorySender, StoryWorker
from indexer.worker import QuarantineException

logger = logging.getLogger("hist-fetcher")

# NOTE! Two database epochs (B & D) have overlapping download ids:
# DB B: 2021-09-15 00:00:00.921164 thru 2021-11-11 00:02:17.402188?
# DB D: 2021-12-26 10:48:42.490858 thru 2022-01-25 00:00:00.072873?

# db-b/stories_2021_09_15.csv:
# 2021-09-15 16:36:53.505277,2043951575,18710,3211617068,59843,https://www.businessinsider.com/eff-google-daily-stormer-2017-8#comments

# DB  attrs from boto (not necessarily story downloads):
# B   3211617604 sbRmtvTcbmDH.dxWNcIBsmRz.ffbbosG 18620 2021-09-15T20:51:01
# B   3211617605 dSQTc9dyyVj34GP7zMI0GfOFmEkaQE_K 13761 2021-09-15T20:50:59
# D   3211617605 fKUnm9Sbr8Gt33FaY0gUoKxaq5J3kY1l 36 2021-12-27T05:11:47
# ....
# B   3257240456 N5Bn9xkkeGgXI1BSSzl71hRi9eiHCMo8 5499 2021-10-16T08:53:20
# D   3257240456 Vp_Qfu7Yo1QkZqWA4RRYu83h0PFoFLOz 8853 2022-01-25T15:47:27
# B   3257240457 N4tYO8GEnt6_px8SHE9VYc5B5N9Yp_hN 36 2021-10-16T08:53:19

OVERLAP_START = 3211617605  # lowest dl_id w/ multiple versions?
OVERLAP_END = 3257240456  # highest dl_id w/ multiple versions?

DB_B_START = "2021-09-15"  # earliest date in DB B
DB_B_END = "2021-11-12"  # latest date in DB B
DB_D_START = "2021-12-26"  # earliest possible date in DB D
DB_D_END = "2022-01-26"

DOWNLOADS_BUCKET = "mediacloud-downloads-backup"
DOWNLOADS_PREFIX = "downloads/"


class HistFetcher(StoryWorker):
    def __init__(self, process_name: str, descr: str):
        super().__init__(process_name, descr)

        # XXX use blobstore?!
        for app in ["HIST", "QUEUER"]:
            region = os.environ.get(f"{app}_S3_REGION")
            access_key_id = os.environ.get(f"{app}_S3_ACCESS_KEY_ID")
            secret_access_key = os.environ.get(f"{app}_S3_SECRET_ACCESS_KEY")
            if region and access_key_id and secret_access_key:
                break

        # None values will check ~/.aws/credentials
        self.s3 = boto3.client(
            "s3",
            region_name=region,
            aws_access_key_id=access_key_id,
            aws_secret_access_key=secret_access_key,
        )

    def pick_version(
        self, dlid: int, s3path: str, fetch_date: Optional[str]
    ) -> Optional[Dict[str, Any]]:
        """
        Determine if download id is in the range where multiple versions
        of the S3 object (named by dlid) can exist, and if so, pick the right
        version.

        returns None or ExtraArgs dict w/ VersionId
        """
        if dlid < OVERLAP_START or dlid > OVERLAP_END:
            return None

        if fetch_date is None:
            raise QuarantineException(f"{dlid}: no fetch_date")

        if DB_B_START < fetch_date < DB_B_END:
            fetch_epoch = "B"
        elif DB_D_START < fetch_date < DB_D_END:
            fetch_epoch = "D"
        else:
            raise QuarantineException(f"{dlid}: unknown epoch for {fetch_date}")

        resp = self.s3.list_object_versions(Bucket=DOWNLOADS_BUCKET, Prefix=s3path)
        for version in resp.get("Versions", []):
            # Dict w/ ETag, Size, StorageClass, Key, VersionID, IsLatest, LastModified (datetime)
            if version["Key"] != s3path:  # paranoia
                break

            lmdate = version["LastModified"].isoformat(sep=" ")

            vid = version["VersionId"]

            logger.debug("dlid %d fd %s lm %s v %s", dlid, fetch_date, lmdate, vid)

            ret = {"VersionID": vid}

            # declare victory if both from same epoch
            # NOT checking how close...
            if fetch_epoch == "B" and DB_B_START < lmdate < DB_B_END:
                return ret

            if fetch_epoch == "D" and DB_D_START < lmdate < DB_D_END:
                return ret

        raise QuarantineException(f"{dlid}: epoch {fetch_epoch} no match?")

    def process_story(self, sender: StorySender, story: BaseStory) -> None:
        rss = story.rss_entry()

        if rss.link:
            # XXX inside try: Quarantine on error?
            dlid = int(rss.link)
        else:
            # XXX count
            return

        s3path = DOWNLOADS_PREFIX + str(dlid)

        # need to have whole story in memory (for Story object),
        # so download to a memory-based file object and decompress
        extras = self.pick_version(dlid, s3path, rss.fetch_date)
        with io.BytesIO() as bio:
            # let any Exception cause retry/quarantine
            self.s3.download_fileobj(
                Bucket=DOWNLOADS_BUCKET, Key=s3path, Fileobj=bio, ExtraArgs=extras
            )

            # XXX inside try? quarantine on error?
            html = gzip.decompress(bio.getbuffer())

        # hist-queuer should have checked URL
        url = story.http_metadata().final_url or ""

        if not self.check_story_length(html, url):
            return  # counted and logged

        logger.info("%d %s: %d bytes", dlid, url, len(html))
        with story.raw_html() as raw:
            raw.html = html

        sender.send_story(story)
        self.incr_stories("success", url)


if __name__ == "__main__":
    run(
        HistFetcher,
        "hist-fetcher",
        "Read CSV of historical stories, fetches HTML from S3 and queues",
    )
