"""
elasticsearch import pipeline worker
"""
import argparse
import logging
import os
import sys
from typing import Any, Dict, List, Mapping, Optional, Union, cast

from elastic_transport import NodeConfig, ObjectApiResponse
from elasticsearch import Elasticsearch
from pika.adapters.blocking_connection import BlockingChannel

from indexer.story import BaseStory
from indexer.worker import StoryWorker, run

logger = logging.getLogger(__name__)


class ElasticsearchConnector:
    def __init__(
        self,
        hosts: Union[
            str, List[Union[str, Mapping[str, Union[str, int]], NodeConfig]], None
        ],
        index_name: str,
    ) -> None:
        self.client = Elasticsearch(hosts)
        self.index_name = index_name
        if self.client and self.index_name:
            if not self.client.indices.exists(index=self.index_name):
                self.client.indices.create(index=self.index_name)

    def index(self, document: Mapping[str, Any]) -> ObjectApiResponse[Any]:
        response: ObjectApiResponse[Any] = self.client.index(
            index=self.index_name, document=document
        )
        return response


class ElasticsearchImporter(StoryWorker):
    def define_options(self, ap: argparse.ArgumentParser) -> None:
        super().define_options(ap)
        elasticsearch_host = os.environ.get("ELASTICSEARCH_HOST")
        index_name = os.environ.get("ELASTICSEARCH_INDEX_NAME")
        ap.add_argument(
            "--elasticsearch-host",
            dest="elasticsearch_host",
            default=elasticsearch_host,
            help="override ELASTICSEARCH_HOST",
        )
        ap.add_argument(
            "--index-name",
            dest="index_name",
            type=str,
            default=index_name,
            help=f"Elasticsearch index name, default {index_name}",
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

            keys_to_skip = ["is_homepage", "is_shortened"]
            data: Mapping[str, Optional[Union[str, bool]]] = {
                k: v for k, v in content_metadata.items() if k not in keys_to_skip
            }
            self.import_story(data)

    def import_story(
        self, data: Mapping[str, Optional[Union[str, bool]]]
    ) -> Optional[ObjectApiResponse[Any]]:
        """
        Import a single story to Elasticsearch
        """
        response = None
        if data:
            try:
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
