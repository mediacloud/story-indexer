"""
elasticsearch import pipeline worker
"""
import argparse
import hashlib
import logging
import os
import sys
from datetime import datetime
from typing import Any, Dict, List, Mapping, Optional, Union, cast

from elastic_transport import NodeConfig, ObjectApiResponse
from elasticsearch import Elasticsearch
from pika.adapters.blocking_connection import BlockingChannel

from indexer.story import BaseStory
from indexer.worker import StoryWorker, run

logger = logging.getLogger(__name__)

shards = int(os.environ.get("ELASTICSEARCH_SHARDS", 1))
replicas = int(os.environ.get("ELASTICSEARCH_REPLICAS", 0))

es_settings = {"number_of_shards": shards, "number_of_replicas": replicas}

es_mappings = {
    "properties": {
        "original_url": {"type": "keyword"},
        "url": {"type": "keyword"},
        "normalized_url": {"type": "keyword"},
        "canonical_domain": {"type": "keyword"},
        "publication_date": {"type": "date"},
        "language": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
        "full_language": {"type": "keyword"},
        "text_extraction": {"type": "keyword"},
        "article_title": {
            "type": "text",
            "fields": {"keyword": {"type": "keyword"}},
        },
        "normalized_article_title": {
            "type": "text",
            "fields": {"keyword": {"type": "keyword"}},
        },
        "text_content": {"type": "text"},
    }
}


class ElasticsearchConnector:
    def __init__(
        self,
        hosts: Union[
            str, List[Union[str, Mapping[str, Union[str, int]], NodeConfig]], None
        ],
        index_names: str,
        mappings: Mapping[str, Any],
        settings: Mapping[str, Any],
    ) -> None:
        self.client = Elasticsearch(hosts)
        self.index_names = index_names
        self.mappings = mappings
        self.settings = settings
        if self.client and self.index_names:
            self.create_indices()

    def create_indices(self) -> None:
        for index_name in self.index_names.split(","):
            if not self.client.indices.exists(index=index_name):
                if self.mappings and self.settings:
                    self.client.indices.create(
                        index=index_name,
                        mappings=self.mappings,
                        settings=self.settings,
                    )
                else:
                    self.client.indices.create(index=index_name)

    def index(
        self, id: str, index_name: str, document: Mapping[str, Any]
    ) -> ObjectApiResponse[Any]:
        response: ObjectApiResponse[Any] = self.client.index(
            index=index_name, id=id, document=document
        )
        return response


class ElasticsearchImporter(StoryWorker):
    def define_options(self, ap: argparse.ArgumentParser) -> None:
        super().define_options(ap)
        elasticsearch_host = os.environ.get("ELASTICSEARCH_HOST")
        index_names = os.environ.get("ELASTICSEARCH_INDEX_NAMES")
        ap.add_argument(
            "--elasticsearch-host",
            dest="elasticsearch_host",
            default=elasticsearch_host,
            help="override ELASTICSEARCH_HOST",
        )
        ap.add_argument(
            "--index-names",
            dest="index_names",
            type=str,
            default=index_names,
            help=f"Elasticsearch index name, default {index_names}",
        )

    def index_routing(self, publication_date_str: Optional[str]) -> str:
        """
        determine the routing index bashed on publication year
        """
        year = (
            datetime.strptime(publication_date_str, "%Y-%m-%d").year
            if publication_date_str
            else None
        )

        routing_index = (
            f"mediacloud_search_text_{year}"
            if year in [2021, 2022, 2023]
            else "mediacloud_search_text_other"
        )

        return routing_index

    def process_args(self) -> None:
        super().process_args()
        assert self.args
        logger.info(self.args)

        elasticsearch_host = self.args.elasticsearch_host
        if not elasticsearch_host:
            logger.fatal("need --elasticsearch-host defined")
            sys.exit(1)

        self.elasticsearch_host = elasticsearch_host

        index_names = self.args.index_names
        if index_names is None:
            logger.fatal("need --index-name defined")
            sys.exit(1)
        self.index_names = index_names

        self.connector = ElasticsearchConnector(
            self.elasticsearch_host,
            self.index_names,
            mappings=es_mappings,
            settings=es_settings,
        )

    def process_story(
        self,
        chan: BlockingChannel,
        story: BaseStory,
    ) -> None:
        """
        Process story and extract metadataurl
        """
        content_metadata = story.content_metadata().as_dict()
        if content_metadata:
            for key, value in content_metadata.items():
                if value is None or value == "":
                    raise ValueError(f"Value for key '{key}' is not provided.")

            url = content_metadata.get("url")
            assert isinstance(url, str)
            url_hash = hashlib.sha256(url.encode("utf-8")).hexdigest()
            keys_to_skip = ["is_homepage", "is_shortened"]
            data: Mapping[str, Optional[Union[str, bool]]] = {
                k: v for k, v in content_metadata.items() if k not in keys_to_skip
            }
            self.import_story(url_hash, data)

    def import_story(
        self,
        url_hash: str,
        data: Mapping[str, Optional[Union[str, bool]]],
    ) -> Optional[ObjectApiResponse[Any]]:
        """
        Import a single story to Elasticsearch
        """
        response = None
        if data:
            publication_date = str(data.get("publication_date"))
            target_index = self.index_routing(publication_date)
            try:
                response = self.connector.index(url_hash, target_index, data)
                if response.get("result") == "created":
                    logger.info(
                        f"Story has been successfully imported. to index {target_index}"
                    )
                    import_status_label = "success"
                else:
                    # Log no imported stories
                    logger.info("Story was not imported.")
                    import_status_label = "failed"
            except Exception as e:
                logger.error(f"Elasticsearch exception: {str(e)}")
                import_status_label = "failed"

        self.incr("imported-stories", labels=[("status", import_status_label)])
        return response


if __name__ == "__main__":
    run(ElasticsearchImporter, "importer", "elasticsearch import worker")
