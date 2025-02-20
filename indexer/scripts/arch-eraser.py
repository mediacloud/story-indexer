import argparse
import logging
import random
import sys
import time
from typing import BinaryIO, List, Optional

import mcmetadata
from elasticsearch import Elasticsearch

from indexer.elastic import ElasticMixin
from indexer.queuer import Queuer

logger = logging.getLogger("arch-eraser")


class ArchEraser(ElasticMixin, Queuer):
    """
    A class for deleting documents from Elasticsearch based on URLs from txt files.
    Supports both single and batch deletion operations with configurable delays.
    """

    HANDLE_GZIP = False
    APP_BLOBSTORE = ""
    MAX_RETRY_TIME = 60

    def __init__(self, process_name: str, descr: str):
        super().__init__(process_name, descr)
        self._es_client: Optional[Elasticsearch] = None
        self.indices: str = ""
        self.min_delay: float = 0
        self.max_delay: float = 0
        self.batch_size: int = 0
        self.successful_operations_count: int = 0
        self.display_stats: bool = True

    def qconnect(self) -> None:
        return

    def check_output_queues(self) -> None:
        return

    def define_options(self, ap: argparse.ArgumentParser) -> None:
        super().define_options(ap)
        ap.add_argument(
            "--batch-size",
            dest="batch_size",
            type=int,
            default=1000,
            help="The number of documents to send in a delete request to Elasticsearch.",
        )
        ap.add_argument(
            "--indices",
            dest="indices",
            help="The name of the Elasticsearch indices to delete from",
        )
        ap.add_argument(
            "--min-delay",
            dest="min_delay",
            type=float,
            default=0.5,
            help="The minimum time to wait between delete operations (default: 0.5 seconds)",
        )
        ap.add_argument(
            "--max-delay",
            dest="max_delay",
            type=float,
            default=3.0,
            help="The maximum time to wait between delete operations (default: 3.0 seconds)",
        )

    def process_args(self) -> None:
        super().process_args()
        assert self.args
        self.indices = self.args.indices
        self.min_delay = self.args.min_delay
        self.max_delay = self.args.max_delay
        self.batch_size = self.args.batch_size

    @property
    def es_client(self) -> Elasticsearch:
        if self._es_client is None:
            self._es_client = self.elasticsearch_client()
        return self._es_client

    def delete_documents(self, urls: List[str]) -> None:
        total_urls = len(urls)
        try:
            ids = []
            for url in urls:
                ids.append(mcmetadata.urls.unique_url_hash(url))
                if len(ids) >= self.batch_size:
                    delete_count = self._delete_documents_by_ids(ids)
                    self._update_stats(delete_count)
                    self._apply_delay()
                    ids.clear()
            if ids:
                delete_count = self._delete_documents_by_ids(ids)
                self._update_stats(delete_count)
        except Exception as e:
            logger.exception(e)
        finally:
            self._log_deletion_stats(total_urls)

    def _delete_documents_by_ids(self, ids: List[str]) -> int | None:
        sec = 1 / 16
        while True:
            try:
                response = self.es_client.delete_by_query(
                    index=self.indices,
                    body={"query": {"terms": {"_id": ids}}},
                    wait_for_completion=True,
                )
                if self.display_stats and response.get("deleted") is not None:
                    return int(response.get("deleted"))
                return None
            except Exception as e:
                self.display_stats = False  # If there is an exception we lose all stats and should not display them
                sec *= 2
                if sec > self.MAX_RETRY_TIME:
                    # If an exception occurs we are going to exit to ensure that the file tracker
                    # doesn't mark a file as processed in the end of processing
                    logger.exception(e)
                    sys.exit(1)
                logger.warning("retry delete operation: after %s(s)", sec)
                time.sleep(sec)

    def _update_stats(self, delete_count: int | None) -> None:
        if delete_count is None or not self.display_stats:
            return
        self.successful_operations_count += delete_count

    def _log_deletion_stats(self, total_urls: int) -> None:
        if self.display_stats:
            if self.successful_operations_count == total_urls:
                log_level = logging.INFO
            else:
                log_level = logging.WARNING
            logger.log(
                log_level,
                "Deleted [%s] out of [%s] documents.",
                self.successful_operations_count,
                total_urls,
            )
        else:
            logger.warning("Unable to get deletion stats")

    def _apply_delay(self) -> None:
        delay = random.uniform(self.min_delay, self.max_delay)
        logger.info("Waiting %0.2f seconds before deleting the next batch...", delay)
        time.sleep(delay)

    def process_file(self, fname: str, fobj: BinaryIO) -> None:
        assert self.args
        urls = []
        with open(fname, "r") as file:
            for line in file:
                urls.append(line.strip())
        logger.info("collected %d urls from %s", len(urls), fname)

        if self.args.dry_run:
            return

        # Not a dry run, do the actual deletion
        logger.warning("deleting %d urls from %s here!", len(urls), fname)
        start_time = time.time()
        self.delete_documents(urls)
        end_time = time.time()
        elapsed_time = end_time - start_time
        logger.info("Time taken: %.2f seconds", elapsed_time)


if __name__ == "__main__":
    app = ArchEraser("arch-eraser", "remove stories loaded from archive files from ES")
    app.main()
