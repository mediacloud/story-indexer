"""
This app provides a one time configuration for the Elasticsearch stack.
Apps that write to Elasticsearch depend on these configurations
Should exit gracefully if configurations already exists in Elasticsearch
"""

import argparse
import os
import sys
import time
from logging import getLogger
from typing import Any

from elasticsearch import Elasticsearch

from indexer.app import App, run
from indexer.elastic import ElasticConfMixin

logger = getLogger("elastic-conf")


class ElasticConf(ElasticConfMixin, App):
    def __init__(self, process_name: str, descr: str):
        super().__init__(process_name, descr)
        self.es_snapshot_s3_bucket = os.environ.get(
            "ELASTICSEARCH_SNAPSHOT_REPO_SETTINGS_BUCKET"
        )
        self.es_snapshot_fs_location = os.environ.get(
            "ELASTICSEARCH_SNAPSHOT_REPO_SETTINGS_LOCATION"
        )
        self.es_snapshot_s3_endpoint = os.environ.get(
            "ELASTICSEARCH_SNAPSHOT_REPO_SETTINGS_ENDPOINT"
        )
        self.max_wait_time = int(os.environ.get("ELASTICSEARCH_MAX_WAIT_TIME", "300"))
        self.wait_interval = int(os.environ.get("ELASTICSEARCH_WAIT_INTERVAL", "10"))

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
        # SLM
        ap.add_argument(
            "--es-snapshot-repo",
            dest="es_snapshot_repo",
            default=os.environ.get("ELASTICSEARCH_SNAPSHOT_REPO") or "",
            help="ES snapshot repository name",
        )
        ap.add_argument(
            "--es-snapshot-repo-type",
            dest="es_snapshot_repo_type",
            default=os.environ.get("ELASTICSEARCH_SNAPSHOT_REPO_TYPE") or "",
            help="ES snapshots type, default fs",
        )

    def process_args(self) -> None:
        super().process_args()
        assert self.args
        required_args = [
            ("shards", "ELASTICSEARCH_SHARD_COUNT"),
            ("replicas", "ELASTICSEARCH_SHARD_REPLICAS"),
            ("ilm_max_age", "ELASTICSEARCH_ILM_MAX_AGE"),
            ("ilm_max_shard_size", "ELASTICSEARCH_ILM_MAX_SHARD_SIZE"),
            ("es_snapshot_repo", "ELASTICSEARCH_SNAPSHOT_REPO"),
            ("es_snapshot_repo_type", "ELASTICSEARCH_SNAPSHOT_REPO_TYPE"),
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
        self.es_snapshot_repo = self.args.es_snapshot_repo
        self.es_snapshot_repo_type = self.args.es_snapshot_repo_type

    def wait_for_elasticsearch_cluster(self, es: Elasticsearch) -> bool:
        """Wait for Elasticsearch cluster to be ready with yellow or green status"""
        logger.info("Waiting for Elasticsearch cluster to be ready...")
        start_time = time.time()

        while time.time() - start_time < self.max_wait_time:
            try:
                if not es.ping():
                    logger.info("Elasticsearch not reachable yet, waiting...")
                    time.sleep(self.wait_interval)
                    continue

                health = es.cluster.health(wait_for_status="green", timeout="5s")
                status = health.get("status", "red")

                if status == "green":
                    logger.info(f"Elasticsearch cluster is ready with status: {status}")
                    return True
                else:
                    logger.info(f"Cluster status is {status}, waiting for green...")

            except Exception as e:
                logger.info("Error checking cluster health: %s, retrying...", e)

            time.sleep(self.wait_interval)

        logger.error(
            "Elasticsearch cluster not ready after %s seconds", self.max_wait_time
        )
        return False

    def main_loop(self) -> None:
        es = self.elasticsearch_client()

        if not self.wait_for_elasticsearch_cluster(es):
            logger.error("Elasticsearch cluster not ready, exiting")
            sys.exit(1)

        index_template_created = self.create_index_template(es)
        ilm_policy_created = self.create_ilm_policy(es)
        alias_created = self.create_initial_index(es)
        slm_policy_created = self.create_slm_policy(es)

        if (
            index_template_created
            and ilm_policy_created
            and alias_created
            and slm_policy_created
        ):
            logger.info("All ES configurations applied successfully.")
        else:
            logger.error("One or more configurations failed. Check logs for details.")
            return

    def repository_exists(self, es: Elasticsearch) -> bool:
        repo_name = self.es_snapshot_repo
        try:
            response = es.snapshot.get_repository(name=repo_name)
            return repo_name in response
        except Exception:
            logger.exception("Error validating repository: %s", repo_name)
        return False

    def register_repository(self, es: Elasticsearch) -> bool:
        repo_type = self.es_snapshot_repo_type
        if repo_type not in ["fs", "s3"]:
            logger.error(
                "Unsupported repository type: '%s'. Must be either 'fs' or 's3'",
                repo_type,
            )
            return False

        if self.repository_exists(es):
            logger.info("%s repository already exists.", repo_type)
            return True

        if repo_type == "s3":
            if not any([self.es_snapshot_s3_bucket, self.es_snapshot_s3_endpoint]):
                logger.error(
                    "Failed to register s3 repository %s: bucket or endpoint required, none provided",
                    self.es_snapshot_repo,
                )
                return False

            settings = {}
            if self.es_snapshot_s3_bucket:
                settings["bucket"] = self.es_snapshot_s3_bucket
            if self.es_snapshot_s3_endpoint:
                settings["endpoint"] = self.es_snapshot_s3_endpoint
        else:  # repo-type=fs
            if self.es_snapshot_fs_location:
                settings = {"location": self.es_snapshot_fs_location}

        try:
            response = es.snapshot.create_repository(
                name=self.es_snapshot_repo,
                body={"type": repo_type, "settings": settings},
            )

            acknowledged = False
            if response:
                acknowledged = response.get("acknowledged", False)
            if acknowledged:
                logger.info(
                    "Successfully registered repository: %s", self.es_snapshot_repo
                )
            else:
                logger.error("Failed to register repository: %s", self.es_snapshot_repo)
            return acknowledged
        except Exception:
            logger.exception("Failed to register repository: %s", self.es_snapshot_repo)
        return False

    def create_index_template(self, es: Elasticsearch) -> Any:
        json_data = self.load_index_template()
        if not json_data:
            logger.error(
                "Elasticsearch create index template: error template not loaded"
            )
            sys.exit(1)
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
        if not json_data:
            logger.error("Elasticsearch create ILM policy: error template not loaded")
            sys.exit(1)
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
        if not json_data:
            logger.error(
                "Elasticsearch create initial index: error template not loaded"
            )
            sys.exit(1)
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

    def create_slm_policy(self, es: Elasticsearch) -> Any:
        CURRENT_POLICY_ID = "bi_weekly_slm"
        repository = self.es_snapshot_repo

        if not self.register_repository(es):
            logger.error(
                "Elasticsearch snapshot repository does not exist: %s",
                self.es_snapshot_repo,
            )
            return False

        json_data = self.load_slm_policy_template(CURRENT_POLICY_ID)
        if not json_data:
            logger.error("Elasticsearch create slm policy: error template not loaded")
            sys.exit(1)
        name = json_data["name"]
        config = json_data["config"]
        schedule = json_data["schedule"]
        retention = json_data["retention"]

        response = es.slm.put_lifecycle(
            policy_id=CURRENT_POLICY_ID,
            config=config,
            name=name,
            schedule=schedule,
            retention=retention,
            repository=repository,
        )
        acknowledged = response.get("acknowledged", False)
        if acknowledged:
            logger.info("SLM policy created successfully.")
        else:
            logger.error("Failed to create SLM policy. Response:%s", response)
        return acknowledged


if __name__ == "__main__":
    run(ElasticConf, "elastic-conf", "Elasticsearch configuration")
