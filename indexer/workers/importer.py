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
from urllib.parse import urlparse

from elastic_transport import NodeConfig, ObjectApiResponse
from elasticsearch import Elasticsearch
from pika.adapters.blocking_connection import BlockingChannel

from indexer.story import BaseStory
from indexer.worker import StoryWorker, run

logger = logging.getLogger(__name__)

shards = int(os.environ.get("ELASTICSEARCH_SHARDS", 1))
replicas = int(os.environ.get("ELASTICSEARCH_REPLICAS", 1))

es_settings = {"number_of_shards": shards, "number_of_replicas": replicas}

es_mappings = {
    "properties": {
        "original_url": {"type": "keyword"},
        "url": {"type": "keyword"},
        "normalized_url": {"type": "keyword"},
        "canonical_domain": {"type": "keyword"},
        "publication_date": {"type": "date", "ignore_malformed": True},
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
        "text_content": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
    }
}


def create_elasticsearch_client(
    hosts: Union[str, List[Union[str, Mapping[str, Union[str, int]], NodeConfig]]],
) -> Elasticsearch:
    if isinstance(hosts, str):
        host_urls = hosts.split(",")

    host_configs: Any = []
    for host_url in host_urls:
        parsed_url = urlparse(host_url)
        host = parsed_url.hostname
        scheme = parsed_url.scheme
        port = parsed_url.port
        if host and scheme and port:
            node_config = NodeConfig(scheme=scheme, host=host, port=port)
            host_configs.append(node_config)

    return Elasticsearch(host_configs)


class ElasticsearchConnector:
    def __init__(
        self,
        hosts: Union[
            str, List[Union[str, Mapping[str, Union[str, int]], NodeConfig]], None
        ],
        mappings: Mapping[str, Any],
        settings: Mapping[str, Any],
    ) -> None:
        assert isinstance(hosts, str)
        self.client = create_elasticsearch_client(hosts)
        self.mappings = mappings
        self.settings = settings

    def create_index(self, index_name: str) -> None:
        if not self.client.indices.exists(index=index_name):
            if self.mappings and self.settings:
                self.client.indices.create(
                    index=index_name,
                    mappings=self.mappings,
                    settings=self.settings,
                )
                logger.info(f"Index '{index_name}' created successfully.")
            else:
                self.client.indices.create(index=index_name)
                logger.info(f"Index '{index_name}' created successfully.")
        else:
            logger.warning(f"Index '{index_name}' already exists. Skipping creation.")

    def index(
        self, id: str, index_name: str, document: Mapping[str, Any]
    ) -> ObjectApiResponse[Any]:
        self.create_index(index_name)
        response: ObjectApiResponse[Any] = self.client.index(
            index=index_name, id=id, document=document
        )
        return response


class ElasticsearchImporter(StoryWorker):
    def define_options(self, ap: argparse.ArgumentParser) -> None:
        super().define_options(ap)
        ap.add_argument(
            "--elasticsearch-hosts",
            dest="elasticsearch_hosts",
            default=os.environ.get("ELASTICSEARCH_HOSTS"),
            help="override ELASTICSEARCH_HOSTS",
        )
        ap.add_argument(
            "--index-name-prefix",
            dest="index_name_prefix",
            type=str,
            default=os.environ.get("ELASTICSEARCH_INDEX_NAME_PREFIX"),
            help="Elasticsearch index name prefix",
        )

    def process_args(self) -> None:
        super().process_args()
        assert self.args
        logger.info(self.args)

        elasticsearch_hosts = self.args.elasticsearch_hosts
        if not elasticsearch_hosts:
            logger.fatal("need --elasticsearch-host defined")
            sys.exit(1)
        self.elasticsearch_hosts = elasticsearch_hosts

        index_name_prefix = self.args.index_name_prefix
        if index_name_prefix is None:
            logger.fatal("need --index-name-prefix defined")
            sys.exit(1)
        self.index_name_prefix = index_name_prefix

        self.connector = ElasticsearchConnector(
            self.elasticsearch_hosts,
            mappings=es_mappings,
            settings=es_settings,
        )

    def index_routing(self, publication_date_str: Optional[str]) -> str:
        """
        determine the routing index bashed on publication year
        """
        year = -1
        if publication_date_str:
            try:
                year = datetime.strptime(publication_date_str, "%Y-%m-%d").year
                current_year = datetime.now().year

                if year > current_year:
                    year = -1
                    logger.warning(
                        f"Publication date greater than current year: {current_year}"
                    )
            except ValueError as e:
                logger.warning(f"Error parsing date: {str(e)}")

        index_name_prefix = os.environ.get("ELASTICSEARCH_INDEX_NAME_PREFIX")
        if year >= 2021:
            routing_index = f"{index_name_prefix}_{year}"
        elif 2008 <= year <= 2020:
            routing_index = f"{index_name_prefix}_older"
        else:
            routing_index = f"{index_name_prefix}_other"

        return routing_index

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
                    logger.error(f"Value for key '{key}' is not provided.")
                    continue

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
            except Exception as e:
                response = None
                logger.error(f"Elasticsearch exception: {str(e)}")

            if response and response.get("result") == "created":
                logger.info(
                    f"Story has been successfully imported. to index {target_index}"
                )
                import_status_label = "success"
            else:
                # Log no imported stories
                logger.info("Story was not imported.")
                import_status_label = "failed"

        self.incr("imported-stories", labels=[("status", import_status_label)])
        return response


if __name__ == "__main__":
    run(ElasticsearchImporter, "importer", "elasticsearch import worker")
