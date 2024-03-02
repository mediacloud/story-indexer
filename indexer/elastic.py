"""
Elastic Search App Mixin
"""

# from indexer.workers.importer

import argparse
import json
import os
import sys
from logging import getLogger
from typing import Any, Union

from elasticsearch import Elasticsearch

from indexer.app import AppProtocol

logger = getLogger(__name__)


class ElasticMixin(AppProtocol):
    """
    mixin class for Apps that use Elastic Search API
    """

    def define_options(self, ap: argparse.ArgumentParser) -> None:
        super().define_options(ap)

        ap.add_argument(
            "--elasticsearch-hosts",
            dest="elasticsearch_hosts",
            default=os.environ.get("ELASTICSEARCH_HOSTS") or "",
            help="comma separated list of ES server URLs",
        )
        ap.add_argument(
            "--elasticsearch-config-dir",
            dest="elasticsearch_config_dir",
            default=os.environ.get("ELASTICSEARCH_CONFIG_DIR") or "",
            help="ES config files dir",
        )

    def elasticsearch_client(self) -> Elasticsearch:
        # maybe take boolean arg or environment variable and call
        # getLogger("elastic_transport.transport").setLevel(logging.WARNING)
        # to avoid log message for each op?

        assert self.args
        hosts = self.args.elasticsearch_hosts
        if not hosts:
            logger.fatal("need --elasticsearch-hosts or ELASTICSEARCH_HOSTS")
            sys.exit(1)

        # Connects immediately, performs failover and retries
        return Elasticsearch(hosts.split(","))

    def _load_template(self, name: str) -> dict | Any:
        file_path = os.path.join(self.elasticsearch_config_dir, name)
        with open(file_path, "r") as file:
            data = file.read()
        return json.loads(data)

    def load_index_template(self) -> Any:
        return self._load_template("create_index_template.json")

    def ilm_policy_data(self) -> Any:
        json_data = self.read_file(template_name="create_ilm_policy.json")
        return json_data

    def initial_index_data(self) -> Any:
        json_data = self.read_file(template_name="create_initial_index.json")
        return json_data
