
"""
schedule Elasticsearch snapshots
"""
import argparse
import logging
import os
import sys
import requests
from datetime import date
from logging import getLogger

from indexer.worker import QApp
from indexer.workers.importer import create_elasticsearch_client


logger = logging.getLogger(__name__)

class ElasticSnapshots(QApp):
    def define_options(self, ap: argparse.ArgumentParser) -> None:
        super().define_options(ap)
        ap.add_argument(
            "--elasticsearch-hosts",
            dest="elasticsearch_hosts",
            default=os.environ.get("ELASTICSEARCH_HOSTS"),
            help="override ELASTICSEARCH_HOSTS",
        )

        ap.add_argument(
            "--elasticsearch-snapshot-repo",
            dest="elasticsearch_snapshot_repo",
            default=os.environ.get("ELASTICSEARCH_SNAPSHOT_REPO"),
            help="override ELASTICSEARCH_SNAPSHOT_REPO",
        )
    
    def process_args(self) -> None:
        super().process_args()
        assert self.args

        if not self.args.elasticsearch_hosts:
            logger.fatal("need --elasticsearch-host defined")
            sys.exit(1)
        
        if not self.args.elasticsearch_snapshot_repo:
            logger.fatal("need --elasticsearch-snapshot-repo defined")
            sys.exit(1)
        
        self.elasticsearch_hosts = self.args.elasticsearch_hosts
        self.elasticsearch_snapshot_repo = self.args.elasticsearch_snapshot_repo
    
    def main_loop(self) -> None:
        _TODAY = date.today().strftime('%Y.%m.%d')
        _ELASTICSEARCH_HOSTS = self.elasticsearch_hosts
        _REPOSITORY_NAME = self.elasticsearch_snapshot_repo
        _SNAPSHOT_NAME = f"snapshot-{_TODAY}"

        client = create_elasticsearch_client(hosts=self.elasticsearch_hosts)
        assert client.ping(), "Failed to connect to Elasticsearch"

        index_names = list(client.indices.get_alias().keys())
        logger.info(f"Starting Snapshot {_SNAPSHOT_NAME}")

        hosts = _ELASTICSEARCH_HOSTS
        if len(hosts) > 1:
            host_url = hosts.split(",")[0]
        else:
            host_url = hosts

        url = f"{host_url}/_snapshot/{_REPOSITORY_NAME}/{_SNAPSHOT_NAME}?wait_for_completion=true"
        headers = {'Content-Type': 'application/json'}
        data = {
            "indices": ",".join(index_names),
            "ignore_unavailable": True,
            "include_global_state": False
        }

        response = requests.put(url, headers=headers, json=data)

        if response.status_code == 200:
            logger.info(f"Successfully completed storing {_SNAPSHOT_NAME} in S3")
        else:
            logger.debug(f"Snapshot creation failed with status code {response.status_code}")

if __name__ == "__main__":
    app = ElasticSnapshots(
        "elastic-snapshosts",
        "Take elasticsearch snapshots regularly (daily)",
    )
    app.main()