"""
Elastic Search App Mixin
"""

# from indexer.workers.importer

import argparse
import json
import os
import socket
import sys
from logging import getLogger
from typing import Any

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

    def process_args(self) -> None:
        super().process_args()
        assert self.args
        self.elasticsearch_hosts = self.args.elasticsearch_hosts

        # Assemble an "opaque id" string to pass in to ES, to identify the
        # stack and this process, visible in ES "tasks" API.  Documentation
        # says to set on a per-client (not per-connection) basis, so do it
        # once. We should try to keep these similar between programs (outside
        # story-indexer) that access ES:
        opaque_toks = ["story-indexer"]

        # the stack name itself is not (BY DESIGN) available to avoid
        # testing it rather than adding a new orthognal variable.
        deployment = os.environ.get("DEPLOYMENT_ID")
        if not deployment:
            deployment = os.environ.get("STATSD_REALM")
        if deployment:
            opaque_toks.append(deployment)
        opaque_toks.append(self.process_name)  # App class
        opaque_toks.append(socket.gethostname().split(".")[0])
        opaque_toks.append(str(os.getpid()))
        self.opaque_id = ".".join(opaque_toks)
        logger.info("opaque_id %s", self.opaque_id)

    def elasticsearch_client(self) -> Elasticsearch:
        # maybe take boolean arg or environment variable and call
        # getLogger("elastic_transport.transport").setLevel(logging.WARNING)
        # to avoid log message for each op?
        if not self.elasticsearch_hosts:
            logger.fatal("need --elasticsearch-hosts or ELASTICSEARCH_HOSTS")
            sys.exit(1)
        print(self.elasticsearch_hosts)
        # Connects immediately, performs failover and retries
        return Elasticsearch(
            self.elasticsearch_hosts.split(","), opaque_id=self.opaque_id
        )


class ElasticConfMixin(ElasticMixin):
    """
    Mixin class for handling Elasticsearch configuration.
    Inherits from ElasticMixin to also provide Elasticsearch client functionality.
    """

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
        self.elasticsearch_config_dir = self.args.elasticsearch_config_dir

        if not self.elasticsearch_config_dir:
            logger.fatal("need --elasticsearch_config_dir or ELASTICSEARCH_CONFIG_DIR")
            sys.exit(1)

    def _load_template(self, name: str) -> dict | Any:
        """
        Load a JSON file from the Elasticsearch configuration directory.
        Args:
            name (str): The name of the file to load.
        Returns:
            dict | Any: The data loaded from the JSON file.
        """
        file_path = os.path.join(self.elasticsearch_config_dir, name)
        with open(file_path, "r") as file:
            data = file.read()
        return json.loads(data)

    def load_index_template(self) -> Any:
        """
        Load the elasticsearch index template from a template JSON file.
        Returns:
            Any: The index template data.
        """
        return self._load_template("create_index_template.json")

    def load_ilm_policy_template(self) -> Any:
        """
        Load the ILM policy template from a JSON file.
        Returns:
            Any: The ILM policy template data.
        """
        return self._load_template("create_ilm_policy.json")

    def load_initial_index_template(self) -> Any:
        """
        Load the initial index template from a JSON file.
        Returns:
            Any: The initial index template data.
        """
        return self._load_template("create_initial_index.json")

    def load_slm_policy_template(self, policy_id: str) -> Any:
        """
        Load the initial SLM Policy from a JSON file.
        Returns:
            Any: SLM policy data.
        """
        return self._load_template(f"{policy_id}_policy.json")
