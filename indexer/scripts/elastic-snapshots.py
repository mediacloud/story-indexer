"""
schedule Elasticsearch snapshots
"""
import argparse
import logging
import os
import sys
from datetime import date
from logging import getLogger
from typing import Any, Dict

from indexer.app import App
from indexer.elastic import ElasticMixin

logger = getLogger("elastic-snapshosts")


class ElasticSnapshots(ElasticMixin, App):
    def define_options(self, ap: argparse.ArgumentParser) -> None:
        super().define_options(ap)
        ap.add_argument(
            "--elasticsearch-snapshot-repo",
            dest="elasticsearch_snapshot_repo",
            default=os.environ.get("ELASTICSEARCH_SNAPSHOT_REPO") or "",
            help="ES snapshot repository name",
        )

    def process_args(self) -> None:
        super().process_args()
        assert self.args
        if not self.args.elasticsearch_snapshot_repo:
            logger.fatal(
                "need --elasticsearch-snapshot-repo or ELASTICSEARCH_SNAPSHOT_REPO"
            )
            sys.exit(1)
        self.elasticsearch_snapshot_repo = self.args.elasticsearch_snapshot_repo

    def main_loop(self) -> None:
        def shards_incr(status: str, count: int) -> None:
            self.incr("snap_shards", value=count, labels=[("status", status)])

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
            shards_info = snapshot_info.get("shards", {})
            status_counts: Dict[str, int] = {
                "failed": shards_info.get("failed", 0),
                "successful": shards_info.get("successful", 0),
            }
            for status, count in status_counts.items():
                shards_incr(status, count)

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
        else:
            self.incr("snap_shards", labels=[("status", "failed")])
            error_message = snapshot_info["failures"]["reason"]
            logger.error("Snapshot creation failed :%s", error_message)


if __name__ == "__main__":
    app = ElasticSnapshots(
        "elastic-snapshosts",
        "Take elasticsearch snapshots regularly (daily)",
    )
    app.main()
