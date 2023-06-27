"""
elasticsearch import pipeline worker
"""
import logging
import os
from typing import Any, Dict, List, Optional, Union, cast

from elasticsearch import Elasticsearch, ElasticsearchException
from pika.adapters.blocking_connection import BlockingChannel

from indexer.story import BaseStory
from indexer.worker import StoryWorker, run

logger = logging.getLogger(__name__)


class ElasticsearchConnector:
    def __init__(self, elasticsearch_host: str, index_name: str):
        self.elasticsearch_client = Elasticsearch(hosts=[elasticsearch_host])
        self.index_name = index_name

    def create_index(self) -> None:
        self.elasticsearch_client.indices.create(index=self.index_name)

    def index_document(self, document: Dict[str, Any]) -> Dict[str, Any]:
        response: Dict[str, Any] = self.elasticsearch_client.index(
            index=self.index_name, body=document
        )
        return response


class ElasticsearchImporter(StoryWorker):
    def __init__(self, connector: ElasticsearchConnector):
        elasticsearch_host = cast(str, os.environ.get("ELASTICSEARCH_HOST"))
        index_name = cast(str, os.environ.get("INDEX_NAME"))
        connector = ElasticsearchConnector(elasticsearch_host, index_name)
        self.connector = connector

    def process_story(
        self,
        chan: BlockingChannel,
        story: BaseStory,
    ) -> None:
        """
        Process story and extract metadata
        """
        content_metadata = story.content_metadata()
        if content_metadata:
            data: Dict[str, Optional[Union[str, bool]]] = {
                "original_url": content_metadata.original_url or "",
                "url": content_metadata.url or "",
                "normalized_url": content_metadata.normalized_url or "",
                "canonical_domain": content_metadata.canonical_domain or "",
                "publication_date": content_metadata.publication_date or "",
                "language": content_metadata.language or "",
                "full_language": content_metadata.full_language or "",
                "text_extraction": content_metadata.text_extraction or "",
                "article_title": content_metadata.article_title or "",
                "normalized_article_title": content_metadata.normalized_article_title
                or "",
                "text_content": content_metadata.text_content or "",
                "is_homepage": content_metadata.is_homepage or False,
                "is_shortened": content_metadata.is_shortened or False,
            }

            if data:
                self.import_story(data)

    def import_story(
        self, data: Dict[str, Optional[Union[str, bool]]]
    ) -> Dict[str, str]:
        """
        Import a single story to Elasticsearch
        """
        es_connector = self.connector
        try:
            if data:
                response = es_connector.index_document(data)
                if response.get("result") == "created":
                    logger.info("Story has been successfully imported.")
                else:
                    # Log no imported stories
                    logger.info("Story was not imported.")
            return response
        except ElasticsearchException as e:
            logger.error(f"Elasticsearch exception: {str(e)}")
            return {}


if __name__ == "__main__":
    run(ElasticsearchImporter, "importer", "elasticsearch import worker")
