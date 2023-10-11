"""
report Elastic Search stats to statsd
"""

# Phil, from rabbitmq-stats.py
# with help from importer

import argparse
import time
from logging import getLogger
from typing import Any, Dict

from elastic_transport import ConnectionError, ConnectionTimeout

from indexer.app import App
from indexer.elastic import (
    add_elasticsearch_hosts,
    check_elasticsearch_hosts,
    create_elasticsearch_client,
)

logger = getLogger("elastic-stats")


class ElasticStats(App):
    def define_options(self, ap: argparse.ArgumentParser) -> None:
        super().define_options(ap)

        add_elasticsearch_hosts(ap)

        ap.add_argument(
            "--interval", type=float, help="reporting interval in seconds", default=60.0
        )

    def main_loop(self) -> None:
        assert self.args
        seconds = self.args.interval

        hosts = self.args.elasticsearch_hosts
        es = create_elasticsearch_client(check_elasticsearch_hosts(hosts))

        while True:
            try:
                print(es.cat.indices(bytes="mb", pri=True))
            except (ConnectionError, ConnectionTimeout) as e:
                logger.debug("indices: %r", e)

            # sleep until top of next period:
            sleep_sec = seconds - time.time() % seconds
            logger.debug("sleep %.6g", sleep_sec)
            time.sleep(sleep_sec)


if __name__ == "__main__":
    app = ElasticStats("elastic-stats", "Send Elastic Search stats to statsd")
    app.main()
