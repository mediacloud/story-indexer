"""
schedule Elasticsearch snapshots
"""
import argparse
import logging
import os
import sys
from datetime import date
from logging import getLogger

import requests

from indexer.app import App
from indexer.elastic import ElasticMixin
from indexer.workers.importer import create_elasticsearch_client

logger = logging.getLogger(__name__)


class ElasticSnapshots(ElasticMixin, App):
    def main_loop(self) -> None:
        _TODAY = date.today().strftime("%Y.%m.%d")
        _REPOSITORY_NAME = self.get_elasticsearch_snapshot
        _SNAPSHOT_NAME = f"snapshot-{_TODAY}"

        client = self.elasticsearch_client()
        assert client.ping(), "Failed to connect to Elasticsearch"

        index_names = list(client.indices.get_alias().keys())
        logger.info(f"Starting Snapshot {_SNAPSHOT_NAME}")

        data = {
            "repository": _REPOSITORY_NAME,
            "snapshot": _SNAPSHOT_NAME,
            "indices": ",".join(index_names),
            "ignore_unavailable": True,
            "include_global_state": False,
            "wait_for_completion": True,
        }
        response = client.snapshot.create(**data)

        if "acknowledged" in response and response["acknowledged"]:
            logger.info(f"Successfully created {_SNAPSHOT_NAME} in S3")
        else:
            logger.debug("Snapshot creation failed with status code")
            if "error" in response:
                error_message = response["error"]["reason"]
                logger.error("Snapshot creation failed :%s", error_message)


if __name__ == "__main__":
    app = ElasticSnapshots(
        "elastic-snapshosts",
        "Take elasticsearch snapshots regularly (daily)",
    )
    app.main()
