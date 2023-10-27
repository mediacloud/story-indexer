"""
Elastic Search App Mixin
"""

# from indexer.workers.importer

import argparse
import os
import sys
from logging import getLogger
from typing import Any, List, Mapping, Optional, Union
from urllib.parse import urlparse

from elastic_transport import NodeConfig, ObjectApiResponse
from elasticsearch import Elasticsearch

from indexer.app import ArgsProtocol

logger = getLogger(__name__)


def create_elasticsearch_client(
    hosts: Union[str, List[Union[str, Mapping[str, Union[str, int]], NodeConfig]]],
) -> Elasticsearch:
    if isinstance(hosts, str):
        host_urls = hosts.split(",")

    host_configs: Any = []
    for host_url in host_urls:
        parsed_url = urlparse(host_url)
        host = parsed_url.hostname
        scheme = parsed_url.scheme
        port = parsed_url.port
        if host and scheme and port:
            node_config = NodeConfig(scheme=scheme, host=host, port=port)
            host_configs.append(node_config)

    return Elasticsearch(host_configs)


class ElasticMixin:
    """
    mixin class for Apps that use Elastic Search API
    """

    def define_options(self: ArgsProtocol, ap: argparse.ArgumentParser) -> None:
        super().define_options(ap)

        ap.add_argument(
            "--elasticsearch-hosts",
            dest="elasticsearch_hosts",
            default=os.environ.get("ELASTICSEARCH_HOSTS") or "",
            help="comma separated list of ES server URLs",
        )

    def elasticsearch_client(self: ArgsProtocol) -> Elasticsearch:
        assert self.args
        hosts = self.args.elasticsearch_hosts
        if not hosts:
            logger.fatal("need --elasticsearch-hosts or ELASTICSEARCH_HOSTS")
            sys.exit(1)

        # Connects immediately, performs failover and retries
        es_client = create_elasticsearch_client(hosts)
        return es_client
