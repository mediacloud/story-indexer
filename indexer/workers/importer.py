"""
elasticsearch import pipeline worker
"""
import argparse
import logging
import os
import sys
from typing import Any, Dict, List, Mapping, Optional, Union, cast

from elastic_transport import ObjectApiResponse
from elasticsearch import Elasticsearch
from pika.adapters.blocking_connection import BlockingChannel

from indexer.story import BaseStory
from indexer.worker import StoryWorker, run

logger = logging.getLogger(__name__)


class ElasticsearchConnector:
    def __init__(
        self,
        elasticsearch_host: Optional[Union[str, Mapping[str, Union[str, int]]]] = None,
        index_name: Optional[str] = None,
    ):
        self.elasticsearch_client = (
            Elasticsearch(hosts=[elasticsearch_host]) if elasticsearch_host else None
        )
        self.index_name = index_name
        if self.elasticsearch_client and self.index_name:
            if not self.elasticsearch_client.indices.exists(index=self.index_name):
                self.create_index()

    def create_index(self) -> None:
        if self.elasticsearch_client and self.index_name:
            self.elasticsearch_client.indices.create(index=self.index_name)

    def index(self, document: Mapping[str, Any]) -> ObjectApiResponse[Any]:
        if self.elasticsearch_client and self.index_name:
            response: ObjectApiResponse[Any] = self.elasticsearch_client.index(
                index=self.index_name, document=document
            )
            return response
        else:
            raise ValueError("Elasticsearch host or index name is not provided.")


class ElasticsearchImporter(StoryWorker):
    def define_options(self, ap: argparse.ArgumentParser) -> None:
        super().define_options(ap)
        elasticsearch_host = os.environ.get("ELASTICSEARCH_HOST")
        index_name = os.environ.get("ELASTICSEARCH_INDEX_NAME")
        ap.add_argument(
            "--elasticsearch-host",
            "-U",
            dest="elasticsearch_host",
            default=elasticsearch_host,
            help="override ELASTICSEARCH_HOST",
        )
        ap.add_argument(
            "--index-name",
            "-I",
            dest="index_name",
            type=str,
            default=index_name,
            help="Elasticsearch index name, default 'mediacloud_search_text'",
        )

    def process_args(self) -> None:
        super().process_args()
        assert self.args
        logger.info(self.args)

        elasticsearch_host = self.args.elasticsearch_host
        if not elasticsearch_host:
            logger.fatal("need --elasticsearch-host defined")
            sys.exit(1)

        self.elasticsearch_host = elasticsearch_host

        index_name = self.args.index_name
        if index_name is None:
            logger.fatal("need --index-name defined")
            sys.exit(1)
        self.index_name = index_name

        self.connector = ElasticsearchConnector(
            self.elasticsearch_host, self.index_name
        )

    def process_story(
        self,
        chan: BlockingChannel,
        story: BaseStory,
    ) -> None:
        """
        Process story and extract metadata
        """
        content_metadata = story.content_metadata().as_dict()
        if content_metadata:
            for key, value in content_metadata.items():
                if value is None or value == "":
                    raise ValueError(f"Value for key '{key}' is not provided.")

            data: Mapping[str, Optional[Union[str, bool]]] = {
                "original_url": content_metadata.get("original_url"),
                "url": content_metadata.get("url"),
                "normalized_url": content_metadata.get("normalized_url"),
                "canonical_domain": content_metadata.get("canonical_domain"),
                "publication_date": content_metadata.get("publication_date"),
                "language": content_metadata.get("language"),
                "full_language": content_metadata.get("full_language"),
                "text_extraction": content_metadata.get("text_extraction"),
                "article_title": content_metadata.get("article_title"),
                "normalized_article_title": content_metadata.get(
                    "normalized_article_title"
                ),
                "text_content": content_metadata.get("text_content"),
            }

            if data:
                self.import_story(data)

    def import_story(
        self, data: Mapping[str, Optional[Union[str, bool]]]
    ) -> ObjectApiResponse[Any]:
        """
        Import a single story to Elasticsearch
        """
        try:
            if data:
                response = self.connector.index(data)
                if response.get("result") == "created":
                    logger.info("Story has been successfully imported.")
                else:
                    # Log no imported stories
                    logger.info("Story was not imported.")
        except Exception as e:
            logger.error(f"Elasticsearch exception: {str(e)}")

        return response


if __name__ == "__main__":
    run(ElasticsearchImporter, "importer", "elasticsearch import worker")
