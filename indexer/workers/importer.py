"""
elasticsearch import pipeline worker
"""
import argparse
import hashlib
import logging
import os
import sys
from datetime import datetime, timedelta
from typing import Any, Dict, List, Mapping, Optional, Union, cast

from elastic_transport import ObjectApiResponse
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import ConflictError, RequestError

from indexer.app import run
from indexer.elastic import ElasticMixin
from indexer.story import BaseStory
from indexer.storyapp import StorySender, StoryWorker
from indexer.worker import QuarantineException

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
        "indexed_date": {"type": "date"},
    }
}


class ElasticsearchConnector:
    def __init__(
        self,
        client: Elasticsearch,
        mappings: Mapping[str, Any],
        settings: Mapping[str, Any],
    ) -> None:
        self.client = client
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
            else:
                self.client.indices.create(index=index_name)
            logger.info("Index '%s' created successfully." % index_name)
        else:
            logger.debug("Index '%s' already exists. Skipping creation." % index_name)

    def index(
        self, id: str, index_name: str, document: Mapping[str, Any]
    ) -> ObjectApiResponse[Any]:
        self.create_index(index_name)
        response: ObjectApiResponse[Any] = self.client.create(
            index=index_name, id=id, document=document
        )
        return response


class ElasticsearchImporter(ElasticMixin, StoryWorker):
    def define_options(self, ap: argparse.ArgumentParser) -> None:
        super().define_options(ap)
        ap.add_argument(
            "--index-name-prefix",
            dest="index_name_prefix",
            type=str,
            default=os.environ.get("ELASTICSEARCH_INDEX_NAME_PREFIX"),
            help="Elasticsearch index name prefix",
        )
        ap.add_argument(
            "--no-output",
            action="store_false",
            dest="output_msgs",
            default=True,
            help="Disable output to archiver",
        )

    def process_args(self) -> None:
        super().process_args()
        assert self.args
        logger.info(self.args)

        index_name_prefix = self.args.index_name_prefix
        if index_name_prefix is None:
            logger.fatal("need --index-name-prefix defined")
            sys.exit(1)
        self.index_name_prefix = index_name_prefix

        self.connector = ElasticsearchConnector(
            self.elasticsearch_client(),
            mappings=es_mappings,
            settings=es_settings,
        )

        self.output_msgs = self.args.output_msgs

    def index_routing(self, publication_date_str: Optional[str]) -> str:
        """
        determine the routing index bashed on publication year
        """
        year = -1
        if publication_date_str:
            try:
                pub_date = datetime.strptime(publication_date_str, "%Y-%m-%d")
                year = pub_date.year
                # check for exceptions of future dates just in case gets past mcmetadata
                if pub_date > datetime.now() + timedelta(days=90):
                    year = -1
            except ValueError as e:
                logger.warning("Error parsing date: '%s" % str(e))
        index_name_prefix = self.index_name_prefix
        if year >= 2021:
            routing_index = f"{index_name_prefix}_{year}"
        elif 2008 <= year <= 2020:
            routing_index = f"{index_name_prefix}_older"
        else:
            routing_index = f"{index_name_prefix}_other"

        return routing_index

    def process_story(self, sender: StorySender, story: BaseStory) -> None:
        """
        Process story and extract metadataurl
        """
        content_metadata = story.content_metadata().as_dict()
        if content_metadata:
            for key, value in content_metadata.items():
                if value is None or value == "":
                    logger.error(f"Value for key '{key}' is not provided.")
                    continue

            keys_to_skip = ["is_homepage", "is_shortened"]

            data: Mapping[str, Optional[Union[str, bool]]] = {
                k: v for k, v in content_metadata.items() if k not in keys_to_skip
            }

            # if publication date is none, fallback to rss_fetcher pub_date
            if data["publication_date"] is None:
                data["publication_date"] = story.rss_entry()["pub_date"]

            response = self.import_story(data)
            if response and self.output_msgs:
                # pass story along to archiver (unless disabled)
                sender.send_story(story)

    def import_story(
        self,
        data: Mapping[str, Optional[Union[str, bool]]],
    ) -> Optional[ObjectApiResponse[Any]]:
        """
        Import a single story to Elasticsearch
        """
        response = None
        if data:
            url = str(data.get("url"))
            url_hash = hashlib.sha256(url.encode("utf-8")).hexdigest()
            # We want actual None, not 'None', if publication_date is missing
            if "publication_date" in data and data["publication_date"] not in [
                None,
                "None",
            ]:
                publication_date = str(data["publication_date"])
            else:
                publication_date = None
            # Add the indexed_date with today's date in ISO 8601 format
            indexed_date = datetime.now().isoformat()
            data = {**data, "indexed_date": indexed_date}
            target_index = self.index_routing(publication_date)
            try:
                response = self.connector.index(url_hash, target_index, data)
            except ConflictError:
                self.incr("stories", labels=[("status", "dups")])
            except RequestError as e:
                self.incr("stories", labels=[("status", "reqerr")])
                raise QuarantineException(repr(e))
            except Exception:
                # Capture other exceptions here
                self.incr("stories", labels=[("status", "failed")])
                raise

            if response and response.get("result") == "created":
                logger.info(
                    f"Story has been successfully imported. to index {target_index}"
                )
                import_status_label = "success"
                self.incr("stories", labels=[("status", import_status_label)])

        return response


if __name__ == "__main__":
    run(ElasticsearchImporter, "importer", "elasticsearch import worker")
