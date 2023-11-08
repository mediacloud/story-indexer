"""
schedule Elasticsearch snapshots
"""
import logging
import os
import sys
from datetime import date
from logging import getLogger
from typing import Any

from indexer.app import App
from indexer.elastic import ElasticSnapshotMixin

logger = logging.getLogger(__name__)


class ElasticSnapshots(ElasticSnapshotMixin, App):
    def process_args(self) -> None:
        super().process_args()
        assert self.args
        assert self.args
        if not self.args.elasticsearch_snapshot_repo:
            logger.fatal(
                "need --elasticsearch-snapshot-repo or ELASTICSEARCH_SNAPSHOT_REPO"
            )
            sys.exit(1)
        self.elasticsearch_snapshot_repo = self.args.elasticsearch_snapshot_repo

    def main_loop(self) -> None:
        _TODAY = date.today().strftime("%Y.%m.%d")
        repository_name = self.elasticsearch_snapshot_repo
        snapshot_name = f"snapshot-{_TODAY}"

        client = self.elasticsearch_client()
        assert client.ping(), "Failed to connect to Elasticsearch"

        index_names = list(client.indices.get_alias().keys())
        indices = ",".join(index_names)

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
            self.incr("snapshots", labels=[("status", "success")])
            shards_info = snapshot_info.get("shards", {})
            logger.info(
                "Snapshot Created Successfully Information - "
                "N: %s "
                "T: %s/%s "
                "S: %s/%s",
                snapshot_name,
                snapshot_info.get("start_time"),
                snapshot_info.get("end_time"),
                shards_info.get("total", 0),
                shards_info.get("failed", 0),
                )
            )
        else:
            self.incr("snapshots", labels=[("status", "failed")])
            error_message = snapshot_info["failures"]["reason"]
            logger.error("Snapshot creation failed :%s", error_message)


if __name__ == "__main__":
    app = ElasticSnapshots(
        "elastic-snapshosts",
        "Take elasticsearch snapshots regularly (daily)",
    )
    app.main()
