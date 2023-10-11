"""
Elastic Search App utilities
"""

# from indexer.workers.importer

import argparse
import os
import sys
from logging import getLogger
from typing import Any, List, Optional
from urllib.parse import urlparse

from elastic_transport import NodeConfig, ObjectApiResponse
from elasticsearch import Elasticsearch

logger = getLogger("elastic-stats")

# would prefer to have an ElasticMixin for Apps, but would need a
# "protocol" to inherit from, so a bunch of functions to call
# from App methods.


def add_elasticsearch_hosts(ap: argparse.ArgumentParser) -> None:
    ap.add_argument(
        "--elasticsearch-hosts",
        dest="elasticsearch_hosts",
        default=os.environ.get("ELASTICSEARCH_HOSTS"),
        help="override ELASTICSEARCH_HOSTS",
    )


def check_elasticsearch_hosts(hosts: Optional[str]) -> str:
    if not hosts:
        logger.fatal("need --elasticsearch-hosts or ELASTICSEARCH_HOSTS")
        sys.exit(1)
    return hosts


def create_elasticsearch_client(hosts_str: str) -> Elasticsearch:
    return Elasticsearch(hosts_str.split(","))  # type: ignore[arg-type]
