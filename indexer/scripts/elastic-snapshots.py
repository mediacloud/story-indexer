"""
schedule Elasticsearch snapshots
"""
import argparse
import logging
import os
import sys
from datetime import date
from logging import getLogger
from typing import Any

from indexer.app import App, ArgsProtocol
from indexer.elastic import ElasticMixin

logger = logging.getLogger(__name__)


class ElasticSnapshots(ElasticMixin, App):
    def get_elasticsearch_snapshot(self: ArgsProtocol) -> Any:
        assert self.args
        if not self.args.elasticsearch_snapshot:
            logger.fatal(
                "need --elasticsearch-snapshot-repo or ELASTICSEARCH_SNAPSHOT_REPO"
            )
            sys.exit(1)
        return self.args.elasticsearch_snapshot

    def main_loop(self) -> None:
        _TODAY = date.today().strftime("%Y.%m.%d")
        repository_name: str = self.get_elasticsearch_snapshot()
        snapshot_name: str = f"snapshot-{_TODAY}"

        client = self.elasticsearch_client()
        assert client.ping(), "Failed to connect to Elasticsearch"

        index_names = list(client.indices.get_alias().keys())
        indices: str = ",".join(index_names)

        logger.info(f"Starting Snapshot {snapshot_name}")
        response = client.snapshot.create(
            repository=repository_name,
            snapshot=snapshot_name,
            indices=indices,
            ignore_unavailable=True,
            include_global_state=True,
            wait_for_completion=True,
        )
        snapshot_info = response.get("snapshot", {})
        if snapshot_info.get("state") == "SUCCESS":
            logger.info(f"Successfully created {snapshot_name} in S3")
            logger.info(f"Start Time: {snapshot_info.get('start_time')}")
            logger.info(f"End Time: {snapshot_info.get('end_time')}")
            shards_info = snapshot_info.get("shards", {})
            logger.info(
                f"Shards - Total: {shards_info.get('total', 0)}, Failed: {shards_info.get('failed', 0)}, Successful: {shards_info.get('successful', 0)}"
            )
        else:
            error_message = snapshot_info["failures"]["reason"]
            logger.error("Snapshot creation failed :%s", error_message)


if __name__ == "__main__":
    app = ElasticSnapshots(
        "elastic-snapshosts",
        "Take elasticsearch snapshots regularly (daily)",
    )
    app.main()
