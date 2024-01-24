"""
configure Elasticsearch
"""

import argparse
import json
import os
import sys
from logging import getLogger
from typing import Any, Dict, List, Union, cast

from elastic_transport import ConnectionError, ConnectionTimeout
from elasticsearch import Elasticsearch

from indexer.app import App, run
from indexer.elastic import ElasticMixin

logger = getLogger("elastic-conf")


class ElasticConf(ElasticMixin, App):
    def define_options(self, ap: argparse.ArgumentParser) -> None:
        super().define_options(ap)
        ap.add_argument(
            "--elasticsearch-config-dir",
            dest="elasticsearch_config_dir",
            default=os.environ.get("ELASTICSEARCH_CONFIG_DIR") or "",
            help="ES config files dir",
        )

    def process_args(self) -> None:
        super().process_args()
        assert self.args
        if not self.args.elasticsearch_config_dir:
            logger.fatal("need --elasticsearch-config-dir or ELASTICSEARCH_CONFIG_DIR")
            sys.exit(1)
        self.elasticsearch_config_dir = self.args.elasticsearch_config_dir

    def main_loop(self) -> None:
        es = self.elasticsearch_client()
        assert es.ping(), "Failed to connect to Elasticsearch"
        ELASTICSEARCH_CONF_DIR = self.elasticsearch_config_dir
        index_template_path = os.path.join(
            ELASTICSEARCH_CONF_DIR, "create_index_template.json"
        )
        ilm_policy_path = os.path.join(ELASTICSEARCH_CONF_DIR, "create_ilm_policy.json")
        initial_index_template = os.path.join(
            ELASTICSEARCH_CONF_DIR, "create_initial_index.json"
        )
        # snapshot_policy_path = "/elasticsearch/conf/create_snapshot_policy.json"

        index_template_created = self.create_index_template(es, index_template_path)
        ilm_policy_created = self.create_ilm_policy(es, ilm_policy_path)
        alias_created = self.create_initial_index(es, initial_index_template)
        # snapshot_policy_created = self.create_snapshot_policy(es, snapshot_policy_path)

        if index_template_created and ilm_policy_created and alias_created:
            logger.info("All ES configurations applied successfully.")
        else:
            logger.error("One or more configurations failed. Check logs for details.")
            return

    def read_file(self, file_path: str) -> Union[dict, Any]:
        with open(file_path, "r") as file:
            data = file.read()
        return json.loads(data)

    def create_index_template(self, es: Elasticsearch, file_path: str) -> bool:
        json_data = self.read_file(file_path)
        name = json_data["name"]
        template = json_data["template"]
        index_patterns = json_data["index_patterns"]

        response = es.indices.put_index_template(
            name=name, index_patterns=index_patterns, template=template
        )
        if response.get("acknowledged", False):
            logger.info("Index template created successfully.")
            return True
        else:
            logger.error("Failed to create index template. Response:%s", response)
            return False

    def create_ilm_policy(self, es: Elasticsearch, file_path: str) -> bool:
        json_data = self.read_file(file_path)
        name = json_data["name"]
        policy = json_data["policy"]
        response = es.ilm.put_lifecycle(name=name, policy=policy)
        if response.get("acknowledged", False):
            logger.info("ILM policy created successfully.")
            return True
        else:
            logger.error("Failed to create ILM policy. Response:%s", response)
            return False

    def create_initial_index(self, es: Elasticsearch, file_path: str) -> bool:
        json_data = self.read_file(file_path)
        index = json_data["name"]
        aliases = json_data["aliases"]
        response = es.indices.create(index=index, aliases=aliases)
        if not es.indices.exists(index=index):
            if response.get("acknowledged", False):
                logger.info("Index created successfully.")
                return True
            else:
                logger.error("Failed to create Index. Response:%s", response)
                return False
        else:
            # Skip the index creation
            logger.warning("Index already exists. Skipping creation.")
            return False


if __name__ == "__main__":
    run(ElasticConf, "elastic-conf", "Elasticsearch configuration")
