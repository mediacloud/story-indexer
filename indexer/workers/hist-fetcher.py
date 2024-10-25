"""
Fetch stories archived on S3 by legacy system
using Stories created from CSV by hist-queuer
(reading CSV files from S3)

Separate from hist-queuer because S3 latency prevents a single process
from getting anything near the (single-prefix) fetch limit of 4500
objects/second, AND to make it so that crashes/interruptions in the
fetch keep track of what has already been handled.
"""

import gzip
import io
import logging
import os
import time
from typing import Any, Dict, Optional

import boto3
from botocore.exceptions import ClientError

from indexer.app import run
from indexer.story import BaseStory
from indexer.storyapp import StorySender, StoryWorker
from indexer.worker import QuarantineException

logger = logging.getLogger("hist-fetcher")

# NOTE! Two database epochs (B & D) have overlapping download ids:
# DB B: 2021-09-15 00:00:00.921164 thru 2021-11-11 00:02:17.402188?
# DB D: 2021-12-26 10:48:42.490858 thru 2022-01-25 00:00:00.072873?

# S3 attrs from boto (not necessarily story downloads):
# ep. downloadid S3 VersionId                     S3siz S3 LastModified
# B   3211617604 sbRmtvTcbmDH.dxWNcIBsmRz.ffbbosG 18620 2021-09-15T20:51:01
# B   3211617605 dSQTc9dyyVj34GP7zMI0GfOFmEkaQE_K 13761 2021-09-15T20:50:59
# D   3211617605 fKUnm9Sbr8Gt33FaY0gUoKxaq5J3kY1l 36    2021-12-27T05:11:47
# ....
# B   3257240456 N5Bn9xkkeGgXI1BSSzl71hRi9eiHCMo8 5499 2021-10-16T08:53:20 (*)
# D   3257240456 Vp_Qfu7Yo1QkZqWA4RRYu83h0PFoFLOz 8853 2022-01-25T15:47:27 (*)
# B   3257240457 N4tYO8GEnt6_px8SHE9VYc5B5N9Yp_hN 36   2021-10-16T08:53:19
#
# (*) db-d/stories_2022_01_25.csv
# 2022-01-25 10:47:25.650489,2076253141,664224,3257240456,1747078,https://oglecountylife.com/article/museum-caps-year-with-christmas-party

OVERLAP_START = 3211617605  # lowest dl_id w/ multiple versions?
OVERLAP_END = 3257240456  # highest dl_id w/ multiple versions?

DB_B_START = "2021-09-15"  # earliest date in DB B
DB_B_END = "2021-11-11"  # latest date in DB B
DB_D_START = "2021-12-26"  # earliest possible date in DB D
DB_D_END = "2022-01-25"  # latest date in DB D

DOWNLOADS_BUCKET = "mediacloud-downloads-backup"
DOWNLOADS_PREFIX = "downloads/"


def date2epoch(date: str) -> Optional[str]:
    if DB_B_START <= date <= DB_B_END:
        return "B"
    if DB_D_START <= date <= DB_D_END:
        return "D"
    return None


class HistFetcher(StoryWorker):
    def __init__(self, process_name: str, descr: str):
        super().__init__(process_name, descr)

        # NOTE: not using indexer.blobstore.S3.s3_client:
        # 1. Historical archive is (currently) on S3
        # 2. This allows defaulting to default keys in ~/.aws/credentials
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
        self, dlid: int, s3path: str, collect_date: str
    ) -> Optional[Dict[str, Any]]:
        """
        If download id is in the range where multiple versions
        of the S3 object (named by dlid) can exist called to
        return ExtraArgs dict w/ VersionId, else return None.

        NOTE! Does NOT compare collect_date and S3 LastModified directly!!
        """
        if collect_date is None:
            raise QuarantineException(f"{dlid}: no collect_date")

        fetch_epoch = date2epoch(collect_date)
        if not fetch_epoch:
            raise QuarantineException(f"{dlid}: unknown epoch for {collect_date}")

        resp = self.s3.list_object_versions(Bucket=DOWNLOADS_BUCKET, Prefix=s3path)
        for version in resp.get("Versions", []):
            # Dict w/ ETag, Size, StorageClass, Key, VersionId, IsLatest, LastModified (datetime), Owner
            if version["Key"] != s3path:  # paranoia (in case prefix match)
                break

            lmdate = version["LastModified"].strftime("%Y-%m-%d")
            lm_epoch = date2epoch(lmdate)
            if not lm_epoch:
                logger.debug("lmdate %s not in either epoch", lmdate)
                continue

            vid = version["VersionId"]
            logger.debug(
                "dlid %d fd %s (%s) lm %s (%s) v %s",
                dlid,
                collect_date,
                fetch_epoch,
                lmdate,
                lm_epoch,
                vid,
            )

            # declare victory if both from same epoch
            # NOT checking how close...
            if fetch_epoch == lm_epoch:
                return {"VersionId": vid}

        logger.warning("%s: epoch %s no matching S3 object found", dlid, fetch_epoch)
        return None

    def process_story(self, sender: StorySender, story: BaseStory) -> None:
        rss = story.rss_entry()
        hmd = story.http_metadata()

        dlid_str = rss.link
        if dlid_str is None or not dlid_str.isdigit():
            self.incr_stories("bad-dlid", dlid_str or "EMPTY")
            raise QuarantineException("bad-dlid")

        # download id as int for version picker
        dlid = int(dlid_str)  # validated above

        # format timestamp (CSV file collect_date) for version picker
        dldate = time.strftime("%Y-%m-%d", time.gmtime(hmd.fetch_timestamp))

        s3path = DOWNLOADS_PREFIX + dlid_str

        # hist-queuer checked URL
        url = story.http_metadata().final_url or ""

        # get ExtraArgs (w/ VersionId) if needed
        extras = None
        if dlid >= OVERLAP_START and dlid <= OVERLAP_END:
            extras = self.pick_version(dlid, s3path, dldate)
            if not extras:
                # no object found for dldate epoch:
                self.incr_stories("epoch-err", url or str(dlid))
                return

        # need to have whole story in memory (for Story object),
        # so download to a memory-based file object and decompress
        with io.BytesIO() as bio:
            try:
                self.s3.download_fileobj(
                    Bucket=DOWNLOADS_BUCKET, Key=s3path, Fileobj=bio, ExtraArgs=extras
                )
            except ClientError as exc:
                # Try to detect non-existent object,
                # let any other Exception cause retry/quarantine
                error = exc.response.get("Error")
                if error and error.get("Code") == "404":
                    self.incr_stories("not-found", url or str(dlid))
                    return
                raise

            # XXX inside try? quarantine on error?
            html = gzip.decompress(bio.getbuffer())

        if html.startswith(b"(redundant feed)"):
            self.incr_stories("redundant", url or str(dlid))
            return

        if not self.check_story_length(html, url):
            return  # counted and logged

        logger.info("%d %s: %d bytes", dlid, url, len(html))
        with story.raw_html() as raw:
            raw.html = html

        with hmd:
            hmd.response_code = 200  # for archiver
            if not hmd.fetch_timestamp:  # no timestamp from queuer
                try:
                    resp = self.s3.head_object(Bucket=DOWNLOADS_BUCKET, Key=s3path)
                    hmd.fetch_timestamp = resp["LastModified"].timestamp()
                except KeyboardInterrupt:
                    raise
                except Exception:
                    pass
        sender.send_story(story)
        self.incr_stories("success", url)


if __name__ == "__main__":
    run(
        HistFetcher,
        "hist-fetcher",
        "Read CSV of historical stories, fetches HTML from S3 and queues",
    )
