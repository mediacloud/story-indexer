import argparse
import json
from logging import getLogger
from typing import Any, Dict, Generator, List, Literal, Optional

import mcmetadata
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

from indexer.app import App
from indexer.elastic import ElasticMixin

logger = getLogger("elastic-update-canonical-domain")


class CanonicalDomainUpdate(ElasticMixin, App):

    def __init__(self, process_name: str, descr: str) -> None:
        super().__init__(process_name, descr)
        self.pit_id: Optional[str] = None
        self.query: Optional[Dict[str, Any]] = {}
        self.keep_alive: str = ""
        self._es_client: Optional[Elasticsearch] = None
        self.batch_size: int = 0
        self.updates_buffer: List[Dict[str, Any]] = []
        self.buffer_size: int = 0
        self.index_name: str = ""
        self.query_format: Literal["DLS", "query_string"] = "query_string"
        self.total_matched_docs: Optional[int] = None

    def define_options(self, ap: argparse.ArgumentParser) -> None:
        super().define_options(ap)
        ap.add_argument(
            "--index",
            dest="index",
            help="The name of the Elasticsearch index to update",
        )

        ap.add_argument(
            "--batch",
            dest="batch_size",
            default=1000,
            help="The number of documents to fetch from Elasticsearch in each batch (default: 1000)",
        )

        ap.add_argument(
            "--buffer",
            dest="buffer_size",
            default=2000,
            help="The maximum number of update operations to buffer before flushing to Elasticsearch",
        )

        ap.add_argument(
            "--query",
            dest="query",
            help="Elasticsearch query string to filter documents for processing",
        )

        ap.add_argument(
            "--format",
            dest="query_format",
            default="query_string",
            choices=["DSL", "query_string"],
            help="The elasticsearch query format (supported values are: [DSL, query_string])",
        )

        ap.add_argument(
            "--keep_alive",
            dest="keep_alive",
            default="1m",
            help="How long should Elasticsearch keep the PIT alive",
        )

    def process_args(self) -> None:
        """
        Process command line arguments and set instance variables.
        """
        super().process_args()
        assert self.args
        args = self.args
        self.index_name = args.index
        self.batch_size = args.batch_size
        self.buffer_size = int(args.buffer_size)
        self.keep_alive = args.keep_alive
        self.query_format = args.query_format
        self.query = self.validate_query(args.query)

    @property
    def es_client(self) -> Elasticsearch:
        if self._es_client is None:
            self._es_client = self.elasticsearch_client()
        return self._es_client

    def validate_query(self, query: str) -> Optional[Dict[str, Any]]:
        """
        Validates the query using the Elasticsearch _validate API, opens Point in time

        Returns:
            Validated query
        """
        validated_query = None
        try:
            if self.query_format == "query_string":
                query_dict = {"query": {"query_string": {"query": query}}}
            else:
                query_dict = json.loads(query)
            validation_result = self.es_client.indices.validate_query(
                index=self.index_name, body=query_dict, explain=True
            )
            if validation_result["valid"]:
                self.pit_id = self.es_client.open_point_in_time(
                    index=self.index_name, keep_alive=self.keep_alive
                ).get("id")
                logger.info("Successfully opened Point-in-Time with ID %s", self.pit_id)
                validated_query = query_dict
            else:
                error_msg = "No detailed explanation is available"
                if "error" in validation_result:
                    error_msg = f"Invalid Query - {validation_result['error']}"
                logger.error(error_msg)
        except json.JSONDecodeError as e:
            logger.exception("Invalid Query: Invalid JSON format - {%s}", e)
        except Exception as e:
            logger.exception("Invalid Query:  Validation error - {%s}", e)
        finally:
            return validated_query

    def fetch_document_count(self) -> Optional[int]:
        """
        Get the total number of documents matching the query

        Returns:
            Total number of matching documents
        """
        try:
            assert self.es_client
            count_response = self.es_client.count(
                index=self.index_name, body=self.query
            )
            count = count_response.get("count")
            if count is not None:
                return int(count)
        except Exception as e:
            logger.exception("Error getting document count: %s", e)
        return None

    def fetch_documents_to_update(self) -> Generator[Dict[str, Any], None, None]:
        """
        Get documents that need to be updated using search_after

        Yields:
            Document dictionaries
        """
        try:
            assert self.query and self.es_client
            self.total_matched_docs = self.fetch_document_count()
            logger.info(
                "Found a total of [%s] documents to update", self.total_matched_docs
            )
            # Add a sort by "_doc" (the most efficient sort order) for "search_after" tracking
            # See https://www.elastic.co/guide/en/elasticsearch/reference/current/sort-search-results.html
            self.query["sort"] = [{"_doc": "asc"}]

            self.query["size"] = self.batch_size
            self.query["pit"] = {"id": self.pit_id, "keep_alive": self.keep_alive}

            search_after = None

            while True:
                if search_after:
                    # Update the query with the last sort values to continue the pagination
                    self.query["search_after"] = search_after

                # Fetch the next batch of documents
                response = self.es_client.search(body=self.query)
                hits = response["hits"]["hits"]

                # Each result will return a PIT ID which may change, thus we just need to update it
                self.pit_id = response.get("pit_id")

                if not hits:
                    break

                for hit in hits:
                    yield {
                        "index": hit["_index"],
                        "source": hit["_source"],
                        "id": hit["_id"],
                    }
                # Since we are sorting in ascending order, lets get the last sort value to use for "search_after"
                search_after = hits[-1]["sort"]
        except Exception as e:
            logger.exception(e)

    def queue_canonical_domain_update(self, doc_data: Dict[str, Any]) -> None:
        """
        Extracts canonical domain from document URL and buffers an update action.
        When the buffer is full, updates are flushed to Elasticsearch.

        Args:
            doc_data: Dictionary containing document data with 'source.url', 'index', and 'id' fields
        """
        try:
            canonical_domain = mcmetadata.urls.canonical_domain(
                doc_data["source"]["url"]
            )

            update_action = {
                "_op_type": "update",
                "_index": doc_data["index"],
                "_id": doc_data["id"],
                "doc": {
                    "canonical_domain": canonical_domain,
                },
            }

            self.updates_buffer.append(update_action)

            if len(self.updates_buffer) >= self.buffer_size:
                self.bulk_update()
        except Exception as e:
            logger.exception("Error processing document %s: %s", doc_data.get("id"), e)

    def bulk_update(self) -> None:
        """
        Flush the buffered updates to Elasticsearch
        """
        if not self.updates_buffer:
            return
        try:
            assert self.es_client
            success, failed = bulk(
                client=self.es_client,
                actions=self.updates_buffer,
                refresh=False,
                raise_on_error=False,
            )
            if isinstance(failed, list):
                failed_count = len(failed)
                for error in failed:
                    logger.error("Failed to update: [%s]", error)
            else:
                failed_count = failed
            logger.info(
                "Bulk update summary: %s successful, %s failed", success, failed_count
            )
        except Exception as e:
            logger.exception("Bulk update failed: %s", e)
        finally:
            # Clear the buffer
            self.updates_buffer = []

    def main_loop(self) -> None:
        try:
            assert self.query and self.es_client
            for doc in self.fetch_documents_to_update():
                self.queue_canonical_domain_update(doc)
        except Exception as e:
            logger.fatal(e)
        finally:
            if self.updates_buffer:
                self.bulk_update()
            if isinstance(self.es_client, Elasticsearch) and self.pit_id:
                response = self.es_client.close_point_in_time(id=self.pit_id)
                if response.get("succeeded"):
                    logger.info(
                        "Successfully closed Point-in-Time with ID %s", self.pit_id
                    )


if __name__ == "__main__":
    app = CanonicalDomainUpdate(
        "elastic-update-canonical-domain", "Updates canonical domain"
    )
    app.main()
