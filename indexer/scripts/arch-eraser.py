"""
Sketch of an "archive eraser"

Reads archive files (possibly from remote "blob stores"), extracts URLs
and removes objects from Elasticsearch.

NOTE!! Does not actually remove the archive files from their
off-site storage location(s)!!!!

This was written to remove stories for late Jan thru early March 2022
(database E) that were initially recovered in 2024 (with some link
rot) thru various means(*), so that stories "blindly" recovered from
S3 (without known URL) where canonical URLs were then extracted.

However, experimention suggested that loading the canonical URLs
without first removing the first attempt could lead to a 10% duplicate
rate (initial vs. final URLs and other URL differences).

From the below, it appears all of the WARC files are available on S3,
with some available from B2 as well (may be cheaper to fetch from B2).

(*) The different ways the URLs were recovered:
1. From synthetic RSS files written at the time for IA (both by the legacy system
   and the then "backup" rss-fetcher.

2. From RSS files blindly extracted from S3 (ignoring the HTML files!) into CSV files of URLs

Stories in index mc_search-00002 and mc_search-00003

All files on S3, some on B2 (starting 2024/05/31)

arch prefix     start           end             archives
mccsv           2024/05/22 -> 2024/06/27        S3/(B2)
mc(rss)         2024/05/27 -> 2024/06/20        S3/(B2) [1]
mcrss           2024/06/20 -> 2024/08/16        S3/B2

(B2) means some of the date range on B2

[1] initial WARC files from RSS files written from 2024/05/27 thru
    2024/06/20 start with mc- (see below):

    THESE SHOULD BE VERIFIED!!! The "via" field in the metadata
    should indicate how the URL was obtained!

    dates                   container name (in WARC filename)
    2024/05/27-2024/05/28   cf94b52abe5a        S3 [154 files]
    2024/05/29-2024/06/04   cefd3fdce464        S3 [882 files]
    2024/06/05              0c501ed61cf4        S3 & B2 [497 files]
    2024/06/05              446d55936e82        S3 & B2 [27 files]
    2024/06/05              cefd3fdce464
    2024/06/06-2024/06/09   0c501ed61cf4
    2024/06/09              7e1b47c305f1        S3 & B2 [1 file]
    2024/06/11-2024/06/20   6c55aaf9daaa

================

This is based on the "Queuer" class, which reads both local and remote
input files, and keeps track of which files have been processed.

No queues are involved (provide any value for --rabbitmq-url or RABBITMQ_URL)

The "tracker" uses SQLite3 (**), and should be multi-process safe,
although this application may experience more contention (SQLite3 does
full-table locks for row creation), and testing should be done (using
--dry-run) with multiple processes running to see if any errors or
exceptions are thrown due to lock contention!

(**) The author doesn't care how you pronounce it, but he says "ess cue ell ite"
(like it's a mineral): https://www.youtube.com/watch?v=Jib2AmRb_rk

Because the files involved span a wide range of dates, and have
various forms, rather than implement fancy wildcard or filtering
support, the idea is to collect all the (full) archive URLs into a
file (or files), and use the "indirect file" feature in the queuer
to read the files of URLs.
"""

import argparse
import logging
import random
import time
from typing import BinaryIO, List, Optional

from elasticsearch import Elasticsearch

from indexer.elastic import ElasticMixin
from indexer.queuer import Queuer
from indexer.story_archive_writer import StoryArchiveReader

logger = logging.getLogger("arch-eraser")


class ArchEraser(ElasticMixin, Queuer):
    APP_BLOBSTORE = "HIST"  # first choice for blobstore conf vars
    HANDLE_GZIP = False  # StoryArchiveReader handles compression

    # don't want to talk to RabbitMQ, but too much work
    # to refactor Queuer into a FileProcessor add-in

    def __init__(self, process_name: str, descr: str):
        super().__init__(process_name, descr)
        self.is_batch_delete: bool = False
        self.keep_alive: str = ""
        self.fetch_batch_size: Optional[int] = None
        self.indices: str = ""
        self.min_delay: float = 0
        self.max_delay: float = 0

    def qconnect(self) -> None:
        return

    def check_output_queues(self) -> None:
        return

    def define_options(self, ap: argparse.ArgumentParser) -> None:
        super().define_options(ap)
        ap.add_argument(
            "--fetch-batch-size",
            dest="fetch_batch_size",
            type=int,
            default=1000,
            help="The number of documents to fetch from Elasticsearch in each batch (default: 1000)",
        )
        ap.add_argument(
            "--indices",
            dest="indices",
            help="The name of the Elasticsearch indices to delete",
        )
        ap.add_argument(
            "--keep-alive",
            dest="keep_alive",
            default="1m",
            help="How long should Elasticsearch keep the PIT alive e.g. 1m -> 1 minute",
        )
        ap.add_argument(
            "--batch-delete",
            dest="is_batch_delete",
            action="store_true",
            default=False,
            help="Enable batch deletion of documents (default: False)",
        )
        ap.add_argument(
            "--min-delay",
            dest="min_delay",
            type=int,
            default=0.5,
            help="The minimum time to wait between delete operations (default: 0.5 seconds)",
        )
        ap.add_argument(
            "--max-delay",
            dest="max_delay",
            type=int,
            default=3.0,
            help="The maximum time to wait between delete operations (default: 3.0 seconds)",
        )

    def process_args(self) -> None:
        """
        Process command line arguments and set instance variables.
        """
        super().process_args()
        assert self.args
        self.fetch_batch_size = self.args.fetch_batch_size
        self.indices = self.args.indices
        self.keep_alive = self.args.keep_alive
        self.is_batch_delete = self.args.is_batch_delete
        self.min_delay = self.args.min_delay
        self.max_delay = self.args.max_delay

    def delete_documents(self, urls: List[Optional[str]]) -> None:
        es = self.elasticsearch_client()
        pit_id = None
        total_deleted = 0
        try:
            pit_id = es.open_point_in_time(
                index=self.indices, keep_alive=self.keep_alive
            ).get("id")
            logger.info("Opened Point-in-Time with ID %s", pit_id)
            query = {
                "size": self.fetch_batch_size,
                "query": {"terms": {"original_url": urls}},
                "pit": {"id": pit_id, "keep_alive": self.keep_alive},
                "sort": [{"_doc": "asc"}],
            }
            search_after = None
            while True:
                if search_after:
                    query["search_after"] = search_after
                # Fetch the next batch of documents
                response = es.search(body=query)
                hits = response["hits"]["hits"]
                # Each result will return a PIT ID which may change, thus we just need to update it
                pit_id = response.get("pit_id")
                if not hits:
                    break
                bulk_actions = []
                for hit in hits:
                    document_index = hit["_index"]
                    document_id = hit["_id"]
                    if self.is_batch_delete:
                        bulk_actions.append(
                            {"delete": {"_index": document_index, "_id": document_id}}
                        )
                    else:
                        es.delete(index=document_index, id=document_id)
                        total_deleted += 1
                        delay = random.uniform(self.min_delay, self.max_delay)
                        logger.info(
                            "Waiting %0.2f seconds before deleting the next document...",
                            delay,
                        )
                        time.sleep(delay)
                if bulk_actions:
                    es.bulk(index=self.indices, body=bulk_actions)
                    total_deleted += len(bulk_actions)
                    delay = random.uniform(self.min_delay, self.max_delay)
                    logger.info(
                        "Waiting %0.2f seconds before deleting the next batch...", delay
                    )
                    time.sleep(delay)
                search_after = hits[-1]["sort"]
        except Exception as e:
            logger.exception(e)
        finally:
            log_level = logging.INFO
            total_urls = len(urls)
            if total_deleted != total_urls:
                log_level = logging.WARNING
            logger.log(
                log_level,
                "Deleted [%s] out of [%s] documents.",
                total_deleted,
                total_urls,
            )
            if isinstance(es, Elasticsearch) and pit_id:
                response = es.close_point_in_time(id=pit_id)
                if response.get("succeeded"):
                    logger.info("Successfully closed Point-in-Time with ID %s", pit_id)

    def process_file(self, fname: str, fobj: BinaryIO) -> None:
        assert self.args
        logger.info("process_file %s", fname)
        # it may be possible to make this faster by NOT using
        # StoryArchiveReader and warcio, but it came for "free":
        reader = StoryArchiveReader(fobj)
        urls = []
        for story in reader.read_stories():
            urls.append(story.content_metadata().url)
        logger.info("collected %d urls from %s", len(urls), fname)
        if not self.args.dry_run:
            logger.warning("deleting %d urls from %s here!", len(urls), fname)
            start_time = time.time()
            self.delete_documents(urls)
            end_time = time.time()
            elapsed_time = end_time - start_time
            logger.info("Time taken: %.2f seconds", elapsed_time)


if __name__ == "__main__":
    app = ArchEraser("arch-eraser", "remove stories loaded from archive files from ES")
    app.main()
