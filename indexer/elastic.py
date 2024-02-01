"""
Elastic Search App Mixin
"""

# from indexer.workers.importer

import argparse
import os
import sys
from logging import getLogger

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
