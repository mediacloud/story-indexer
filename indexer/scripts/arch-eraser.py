import argparse
import logging
import random
import sys
import time
from typing import Any, BinaryIO, Dict, Generator, List, Optional

import mcmetadata
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

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
        self.pit_id: Optional[str] = None
        self.is_batch_delete: bool = False
        self.keep_alive: str = ""
        self.fetch_batch_size: Optional[int] = None
        self.indices: str = ""
        self.min_delay: float = 0
        self.max_delay: float = 0
        self.buffer: List[Dict[str, Any]] = []
        self.buffer_size: int = 0
        self.successful_operations_count: int = 0

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
            "--buffer",
            dest="buffer_size",
            type=int,
            default=2000,
            help="The maximum number of delete operations to buffer before flushing to Elasticsearch",
        )
        ap.add_argument(
            "--indices",
            dest="indices",
            help="The name of the Elasticsearch indices to delete from",
        )
        ap.add_argument(
            "--keep-alive",
            dest="keep_alive",
            default="1m",
            help="How long should Elasticsearch keep the PIT alive e.g. 1m -> 1 minute",
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
        self.fetch_batch_size = self.args.fetch_batch_size
        self.indices = self.args.indices
        self.keep_alive = self.args.keep_alive
        self.min_delay = self.args.min_delay
        self.max_delay = self.args.max_delay
        self.buffer_size = self.args.buffer_size

    @property
    def es_client(self) -> Elasticsearch:
        if self._es_client is None:
            self._es_client = self.elasticsearch_client()
        return self._es_client

    def delete_documents(self, urls: List[str]) -> None:
        self._open_pit()
        try:
            total_urls = len(urls)
            ids = []
            for url in urls:
                ids.append(mcmetadata.urls.unique_url_hash(url))

            for hit in self._fetch_documents_to_delete(ids):
                self.queue_delete_op(hit)
            if self.buffer:
                self._bulk_delete()
                self._apply_delay()

            if self.successful_operations_count != total_urls:
                log_level = logging.WARNING
            else:
                log_level = logging.INFO
            logger.log(
                log_level,
                "Deleted [%s] out of [%s] documents.",
                self.successful_operations_count,
                total_urls,
            )
        except Exception as e:
            logger.exception(e)
        finally:
            self._close_pit()

    def _fetch_documents_to_delete(
        self, ids: List[str]
    ) -> Generator[Dict[str, Any], None, None]:
        try:
            query = {
                "size": self.fetch_batch_size,
                "query": {"terms": {"_id": ids}},
                "pit": {"id": self.pit_id, "keep_alive": self.keep_alive},
                "sort": [{"_doc": "asc"}],
            }
            search_after = None
            while True:
                if search_after:
                    query["search_after"] = search_after
                # Fetch the next batch of documents
                response = self.es_client.search(body=query)
                hits = response["hits"]["hits"]
                # Each result will return a PIT ID which may change, thus we just need to update it
                self.pit_id = response.get("pit_id")
                if not hits:
                    break
                for hit in hits:
                    yield {
                        "_index": hit["_index"],
                        "_id": hit["_id"],
                    }
                search_after = hits[-1]["sort"]
        except Exception as e:
            logger.exception(e)

    def queue_delete_op(self, hit: Dict[str, Any]) -> None:
        try:
            delete_action = {
                "_op_type": "delete",
                "_index": hit["_index"],
                "_id": hit["_id"],
            }
            self.buffer.append(delete_action)
            if len(self.buffer) >= self.buffer_size:
                self._bulk_delete()
                self._apply_delay()
        except Exception as e:
            logger.exception("Error processing document %s: %s", hit.get("id"), e)

    def _bulk_delete(self) -> None:
        if not self.buffer:
            return
        sec = 1 / 16
        while True:
            try:
                assert self.es_client
                success, _ = bulk(
                    client=self.es_client,
                    actions=self.buffer,
                    refresh=False,
                    raise_on_error=False,
                )
                self.successful_operations_count += success
                self.buffer = []
                return
            except Exception as e:
                sec *= 2
                if sec > self.MAX_RETRY_TIME:
                    # If an exception occurs we are going to exit to ensure that the file tracker
                    # doesn't mark a file as processed in the end of processing
                    logger.exception(e)
                    sys.exit(1)
                logger.warning("retry bulk delete: after %s(s)", sec)
                time.sleep(sec)

    def _open_pit(self) -> None:
        response = self.es_client.open_point_in_time(
            index=self.indices, keep_alive=self.keep_alive
        )
        self.pit_id = response.get("id")
        logger.info("Opened Point-in-Time with ID %s", self.pit_id)

    def _close_pit(self) -> None:
        if isinstance(self.es_client, Elasticsearch) and self.pit_id:
            response = self.es_client.close_point_in_time(id=self.pit_id)
            if response.get("succeeded"):
                logger.info("Successfully closed Point-in-Time with ID %s", self.pit_id)

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
