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


class ElasticsearchConnector:
    def __init__(self, client: Elasticsearch) -> None:
        self.client = client

    def index(
        self, id: str, index_name_alias: str, document: Mapping[str, Any]
    ) -> ObjectApiResponse[Any]:
        response: ObjectApiResponse[Any] = self.client.create(
            index=index_name_alias, id=id, document=document
        )
        return response


class ElasticsearchImporter(ElasticMixin, StoryWorker):
    def define_options(self, ap: argparse.ArgumentParser) -> None:
        super().define_options(ap)
        ap.add_argument(
            "--index-name-alias",
            dest="index_name_alias",
            type=str,
            default=os.environ.get("ELASTICSEARCH_INDEX_NAME_ALIAS"),
            help="Elasticsearch index names alias",
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

        index_name_alias = self.args.index_name_alias
        if index_name_alias is None:
            logger.fatal("need --index-name-alias defined")
            sys.exit(1)

        self.index_name_alias = index_name_alias
        self.connector = ElasticsearchConnector(self.elasticsearch_client())
        self.output_msgs = self.args.output_msgs

    def process_story(self, sender: StorySender, story: BaseStory) -> None:
        """
        Process story and extract metadataurl
        """
        content_metadata = story.content_metadata().as_dict()
        if content_metadata:
            for key, value in content_metadata.items():
                if value is None or value == "":
                    logger.warning(f"Value for key '{key}' is not provided.")
                    continue

            keys_to_skip = ["is_homepage", "is_shortened"]

            data: Mapping[str, Optional[Union[str, bool]]] = {
                k: v for k, v in content_metadata.items() if k not in keys_to_skip
            }

            # if publication date is none, fallback to rss_fetcher pub_date
            if data["publication_date"] is None:
                pub_date = story.rss_entry().pub_date
                if pub_date is not None or pub_date != "":
                    data = {**data, "publication_date": pub_date}

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
            ## This was needed for index routing, won't be necessary for ILM
             
            # We want actual None, not 'None', if publication_date is missing
            # if "publication_date" in data and data["publication_date"] not in [
            #     None,
            #     "None",
            # ]:
            #     publication_date = str(data["publication_date"])
            # else:
            #     publication_date = None

            # Add the indexed_date with today's date in ISO 8601 format
            indexed_date = datetime.now().isoformat()
            data = {**data, "indexed_date": indexed_date}

            target_index = self.index_name_alias
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
