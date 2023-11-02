"""
schedule Elasticsearch snapshots
"""
import logging
from datetime import date
from logging import getLogger
from typing import Any

from indexer.app import App
from indexer.elastic import ElasticSnapshotMixin

logger = logging.getLogger(__name__)


class ElasticSnapshots(ElasticSnapshotMixin, App):
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
            self.incr("snapshots", labels=[("status", "success")])
            shards_info = snapshot_info.get("shards", {})
            logger.info(
                f"Snapshot Created Successfully Information - "
                f"N: {snapshot_name} "
                f"T: {snapshot_info.get('start_time')}/{snapshot_info.get('end_time')} "
                f"S: {shards_info.get('total', 0)}/{shards_info.get('failed', 0)}"
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
