"""
configure Elasticsearch
"""

import argparse
import json
import subprocess
import time
from collections import Counter
from logging import getLogger
from typing import Any, Dict, List, cast

from elastic_transport import ConnectionError, ConnectionTimeout

from elasticsearch import Elasticsearch
from indexer.app import App
from indexer.elastic import ElasticMixin

logger = getLogger("elastic-stats")


class ElasticConf(ElasticMixin, App):
    def main_loop(self) -> None:
        # maybe:
        # getLogger("elastic_transport.transport").setLevel(logging.WARNING)
        # to avoid log message for each GET?

        es = self.elasticsearch_client()
        assert es.ping(), "Failed to connect to Elasticsearch"

        index_template_path = "./elasticsearch/conf/create_index_template.json"
        ilm_policy_path = "./elasticsearch/conf/create_ilm_policy.json"
        initial_index_template = "./elasticsearch/conf/create_initial_index.json"
        # snapshot_policy_path = "/elasticsearch/conf/create_snapshot_policy.json"

        index_template_created = self.create_index_template(es, index_template_path)
        ilm_policy_created = self.create_ilm_policy(es, ilm_policy_path)
        alias_created = self.create_initial_index(es, initial_index_template)
        # snapshot_policy_created = self.create_snapshot_policy(es, snapshot_policy_path)

        if index_template_created and ilm_policy_created and alias_created:
            logger.info("All ES configurations applied successfully.")
        else:
            logger.error("One or more configurations failed. Check logs for details.")

    def create_index_template(self, es: Elasticsearch, file_path: str) -> bool:
        with open(file_path, "r") as file:
            data = file.read()

        json_data = json.loads(data)
        name = json_data.get("name")
        template = json_data.get("template")
        index_patterns = json_data.get("index_patterns")

        # Check if the index template already exists
        if es.indices.exists_index_template(name=name):
            logger.info("Index template ':%s' already exists. Skipping creation.", name)
            return True

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
        with open(file_path, "r") as file:
            data = file.read()

        json_data = json.loads(data)
        name = json_data.get("name")
        policy = json_data.get("policy")
        response = es.ilm.put_lifecycle(name=name, policy=policy)
        if response.get("acknowledged", False):
            logger.info("ILM policy created successfully.")
            return True
        else:
            logger.error("Failed to create ILM policy. Response:%s", response)
            return False

    def create_initial_index(self, es: Elasticsearch, file_path: str) -> bool:
        with open(file_path, "r") as file:
            data = file.read()
        json_data = json.loads(data)
        index = json_data.get("name")
        aliases = json_data.get("aliases")
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
    app = ElasticConf(
        "elastic-conf",
        "Elasticsearch conf",
    )
    app.main()
