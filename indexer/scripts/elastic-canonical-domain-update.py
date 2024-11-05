import argparse
import json
from logging import getLogger
from typing import Any, Dict, Generator, List, Optional, Tuple

import mcmetadata
from elasticsearch.helpers import bulk, scan

from indexer.app import App
from indexer.elastic import ElasticMixin

logger = getLogger("elastic-canonical-domain-updater")


class CanonicalDomainUpdater(ElasticMixin, App):

    def __init__(self, process_name: str, descr: str) -> None:
        super().__init__(process_name, descr)
        self.batch_size: int = 0
        self.updates_buffer: List[Dict[str, Any]] = []
        self.buffer_size: int = 0
        self.index_name: str = ""
        self.query: str = ""
        self.total_matched_docs: Optional[int] = None

    def define_options(self, ap: argparse.ArgumentParser) -> None:
        """
        Define command line arguments for the script.
        Extends the parent class argument definitions.

        Args:
            ap (argparse.ArgumentParser): The argument parser instance to add arguments to

        Adds the following arguments:
            --index: Name of the Elasticsearch index to update
            --batch: Batch size for document fetching (default: 1000)
            --buffer: Size of update operation buffer (default: 2000)
            --query: Elasticsearch query string for filtering documents
        Returns:
            None
        """
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

    def process_args(self) -> None:
        """
        Process command line arguments and set instance variables.

        Sets the following instance attributes:
            index_name (str): Name of the Elasticsearch index to operate on
            batch_size (int): Number of documents to retrieve per batch from Elasticsearch
            buffer_size (int): Maximum number of update operations to buffer before flushing to Elasticsearch
            query (str): Elasticsearch query string to filter documents for processing

        Note:
            Calls the parent class's process_args() first to handle any inherited argument processing.
        """
        super().process_args()

        args = self.args
        assert args

        self.index_name = args.index
        self.batch_size = args.batch_size
        self.buffer_size = int(args.buffer_size)
        self.query = args.query

    def initialize(self) -> None:
        """
        Initializes the Elasticsearch instance and sets up application arguments.
        Returns:
             None
        """
        parser = argparse.ArgumentParser()
        app.define_options(parser)
        app.args = parser.parse_args()
        app.process_args()
        self.es_client = self.elasticsearch_client()

    def build_query(self) -> Tuple[bool, Optional[Dict[str, Any]], Optional[str]]:
        """
        Builds a query for searching documents whose canonical domain must be updated, the query is validated by
        the Elasticsearch _validate API, before execution

        Returns:
            Tuple of (is_valid, parsed_query, error_message)
        """
        try:
            # Try to parse the string as JSON first
            query = json.loads(self.query)

            # Extract just the query part if it exists
            query_body = query.get("query", query)

            # Use the Elasticsearch validate API to ensure the query is ok
            validation_result = self.es_client.indices.validate_query(
                index=self.index_name, body={"query": query_body}, explain=True
            )

            if validation_result["valid"]:
                return True, query, None
            else:
                error_msg = "No detailed explanation is available"
                if "error" in validation_result:
                    error_msg = f"Invalid Query - {validation_result['error']}"
                return False, None, error_msg
        except json.JSONDecodeError as e:
            return False, None, f"Invalid Query: Invalid JSON format - {str(e)}"
        except Exception as e:
            return False, None, f"Invalid Query:  Validation error - {str(e)}"

    def get_document_count(self, query: Dict[str, Any]) -> Optional[int]:
        """
        Get the total number of documents matching the query

        Args:
            query: Elasticsearch query

        Returns:
            Total number of matching documents
        """
        try:
            count_response = self.es_client.count(index=self.index_name, body=query)
            count = count_response.get("count")
            if count is not None:
                return int(count)
        except Exception as e:
            logger.error(f"Error getting document count: {e}")
        return None

    def get_documents_to_update(self) -> Generator[Dict[str, Any], None, None]:
        """
        Get documents that need to be updated using scroll API

        Yields:
            Document dictionaries
        """
        try:
            # Build the query from the string passed by the user as an arg, the build process also validates the query
            success, query, error = self.build_query()

            if success:
                assert query
                self.total_matched_docs = self.get_document_count(query)
                logger.info(
                    f"Found a total of [{self.total_matched_docs}] documents to update."
                )

                for hit in scan(
                    client=self.es_client,
                    query=query,
                    index=self.index_name,
                    size=self.batch_size,
                    preserve_order=True,
                ):
                    yield {
                        "index": hit["_index"],
                        "source": hit["_source"],
                        "id": hit["_id"],
                    }
            else:
                raise Exception(error)

        except Exception as e:
            logger.error(e)

    def queue_canonical_domain_update(self, doc_data: Dict[str, Any]) -> None:
        """
        Extracts canonical domain from document URL and buffers an update action.
        When the buffer is full, updates are flushed to Elasticsearch.

        Args:
            doc_data: Dictionary containing document data with 'source.url', 'index', and 'id' fields
        """
        try:
            # Determine the canonical domain from mcmetadata, the URL we have on these document is a canonical URL
            canonical_domain = mcmetadata.urls.canonical_domain(
                doc_data["source"]["url"]
            )

            # Create an update action where we update only the canonical domain by Document ID
            update_action = {
                "_op_type": "update",
                "_index": doc_data["index"],
                "_id": doc_data["id"],
                "doc": {
                    "canonical_domain": canonical_domain,
                },
            }

            self.updates_buffer.append(update_action)

            # Check if the buffer is full and flush updates
            if len(self.updates_buffer) >= self.buffer_size:
                self.flush_updates()
        except Exception as e:
            logger.error(f"Error processing document {doc_data.get('id')}: {e}")

    def flush_updates(self) -> None:
        """
        Flush the buffered updates to Elasticsearch
        """
        if not self.updates_buffer:
            return
        try:
            # Perform bulk update
            success, failed = bulk(
                client=self.es_client,
                actions=self.updates_buffer,
                refresh=False,
                raise_on_error=False,
            )

            is_failed_list = isinstance(failed, list)
            if is_failed_list:
                assert isinstance(failed, list)
                failed_count = len(failed)
            else:
                assert isinstance(failed, int)
                failed_count = failed
            logger.info(f"Bulk update: {success} successful, {failed_count} failed")

            if is_failed_list:
                assert isinstance(failed, list)
                for error in failed:
                    logger.error(f"Failed to update: {error}")
        except Exception as e:
            logger.error(f"Bulk update failed: {e}")
        finally:
            # Clear the buffer
            self.updates_buffer = []

    def main(self) -> None:
        """
        Main execution method for processing canonical domain updates to documents.

        This method serves as the entry point for the application logic. It initializes the
        application, retrieves documents that require updates, and processes them by queuing
        updates for the canonical domain. Any exceptions encountered during execution are
        logged as fatal errors. Finally, it ensures that any remaining updates in the buffer
        are flushed before execution completes.

        Returns:
            None
        """
        try:
            # Initialize the app
            self.initialize()
            # Get a buffer of documents that need to be updated
            for doc in self.get_documents_to_update():
                self.queue_canonical_domain_update(doc)
        except Exception as e:
            logger.fatal(e)
        finally:
            # Ensure the buffer is flushed if it's not empty before finishing execution
            if self.updates_buffer:
                self.flush_updates()


if __name__ == "__main__":
    app = CanonicalDomainUpdater(
        "elastic-canonical-domain-updater", "Updates canonical domain"
    )
    app.main()
