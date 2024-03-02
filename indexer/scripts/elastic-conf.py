"""
This app provides a one time configuration for the Elasticsearch stack.
Apps that write to Elasticsearch depend on these configurations
Should exit gracefully if configurations already exists in Elasticsearch
"""

import argparse
import os
import sys
from logging import getLogger
from typing import Any

from elasticsearch import Elasticsearch

from indexer.app import App, run
from indexer.elastic import ElasticMixin

logger = getLogger("elastic-conf")


class ElasticConf(ElasticMixin, App):
    def define_options(self, ap: argparse.ArgumentParser) -> None:
        super().define_options(ap)
        # Index template args
        ap.add_argument(
            "--shards",
            dest="shards",
            default=os.environ.get("ELASTICSEARCH_SHARD_COUNT") or "",
            help="ES number of shards",
        )
        ap.add_argument(
            "--replicas",
            dest="replicas",
            default=os.environ.get("ELASTICSEARCH_SHARD_REPLICAS") or "",
            help="ES number of replicas",
        )
        # ILM policy args
        ap.add_argument(
            "--ilm-max-age",
            dest="ilm_max_age",
            default=os.environ.get("ELASTICSEARCH_ILM_MAX_AGE") or "",
            help="ES ILM policy max age",
        )
        ap.add_argument(
            "--ilm-max-shard-size",
            dest="ilm_max_shard_size",
            default=os.environ.get("ELASTICSEARCH_ILM_MAX_SHARD_SIZE") or "",
            help="ES ILM policy max shard size",
        )

    def process_args(self) -> None:
        super().process_args()
        assert self.args
        required_args = [
            ("shards", "ELASTICSEARCH_SHARD_COUNT"),
            ("replicas", "ELASTICSEARCH_SHARD_REPLICAS"),
            ("ilm_max_age", "ELASTICSEARCH_ILM_MAX_AGE"),
            ("ilm_max_shard_size", "ELASTICSEARCH_ILM_MAX_SHARD_SIZE"),
        ]
        for arg_name, env_name in required_args:
            arg_val = getattr(self.args, arg_name)
            if not arg_val:
                logger.fatal(f"need --{arg_name} or {env_name}")
                sys.exit(1)

        self.shards = self.args.shards
        self.replicas = self.args.replicas
        self.ilm_max_age = self.args.ilm_max_age
        self.ilm_max_shard_size = self.args.ilm_max_shard_size

    def main_loop(self) -> None:
        es = self.elasticsearch_client()
        assert es.ping(), "Failed to connect to Elasticsearch"
        index_template_created = self.create_index_template(es)
        ilm_policy_created = self.create_ilm_policy(es)
        alias_created = self.create_initial_index(es)

        if index_template_created and ilm_policy_created and alias_created:
            logger.info("All ES configurations applied successfully.")
        else:
            logger.error("One or more configurations failed. Check logs for details.")
            return

    def create_index_template(self, es: Elasticsearch) -> Any:
        json_data = self.load_index_template()
        json_data["template"]["settings"]["number_of_shards"] = self.shards
        json_data["template"]["settings"]["number_of_replicas"] = self.replicas
        name = json_data["name"]
        template = json_data["template"]
        index_patterns = json_data["index_patterns"]

        response = es.indices.put_index_template(
            name=name, index_patterns=index_patterns, template=template
        )

        acknowledged = response.get("acknowledged", False)
        if acknowledged:
            logger.info("Index template created successfully.")
        else:
            logger.error("Failed to create index template. Response: %s", response)
        return acknowledged

    def create_ilm_policy(self, es: Elasticsearch) -> Any:
        json_data = self.load_ilm_policy_template()
        rollover = json_data["policy"]["phases"]["hot"]["actions"]["rollover"]
        rollover["max_age"] = self.ilm_max_age
        rollover["max_primary_shard_size"] = self.ilm_max_shard_size
        name = json_data["name"]
        policy = json_data["policy"]
        response = es.ilm.put_lifecycle(name=name, policy=policy)
        acknowledged = response.get("acknowledged", False)
        if acknowledged:
            logger.info("ILM policy created successfully.")
        else:
            logger.error("Failed to create ILM policy. Response:%s", response)
        return acknowledged

    def create_initial_index(self, es: Elasticsearch) -> Any:
        json_data = self.load_initial_index_template()
        index = json_data["name"]
        aliases = json_data["aliases"]
        if es.indices.exists(index=index):
            logger.warning("Index already exists. Skipping creation.")
            return True
        else:
            response = es.indices.create(index=index, aliases=aliases)
            acknowledged = response.get("acknowledged", False)
            if acknowledged:
                logger.info("Index created successfully.")
            else:
                logger.error("Failed to create Index. Response:%s", response)
            return acknowledged


if __name__ == "__main__":
    run(ElasticConf, "elastic-conf", "Elasticsearch configuration")
