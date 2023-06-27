"""
elasticsearch import pipeline worker
"""
import logging
import os
from typing import Any, Dict, List, Optional, Union, cast

from elasticsearch import ElasticsearchException
from pika.adapters.blocking_connection import BlockingChannel

from indexer.elastic_conf import ElasticsearchConnector
from indexer.story import BaseStory
from indexer.worker import StoryWorker, run

logger = logging.getLogger(__name__)


class ElasticsearchImporter(StoryWorker):
    def __init__(self, connector: ElasticsearchConnector):
        elasticsearch_host = cast(str, os.environ.get("ELASTICSEARCH_HOST"))
        index_name = cast(str, os.environ.get("INDEX_NAME"))
        self.connector = ElasticsearchConnector(elasticsearch_host, index_name)

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
                "original_url": content_metadata.original_url,
                "url": content_metadata.url,
                "normalized_url": content_metadata.normalized_url,
                "canonical_domain": content_metadata.canonical_domain,
                "publication_date": content_metadata.publication_date,
                "language": content_metadata.language,
                "full_language": content_metadata.full_language,
                "text_extraction": content_metadata.text_extraction,
                "article_title": content_metadata.article_title,
                "normalized_article_title": content_metadata.normalized_article_title,
                "text_content": content_metadata.text_content,
                "is_homepage": content_metadata.is_homepage,
                "is_shortened": content_metadata.is_shortened,
            }
            data = {k: v for k, v in data.items() if v is not None}

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
