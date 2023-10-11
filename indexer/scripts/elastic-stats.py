"""
report Elastic Search stats to statsd
"""

# Phil, from rabbitmq-stats.py
# with help from importer

import argparse
import time
from collections import Counter
from logging import getLogger
from typing import Any, Dict, List, cast

from elastic_transport import ConnectionError, ConnectionTimeout

from indexer.app import App
from indexer.elastic import ElasticMixin

logger = getLogger("elastic-stats")


class ElasticStats(ElasticMixin, App):
    def define_options(self, ap: argparse.ArgumentParser) -> None:
        super().define_options(ap)

        ap.add_argument(
            "--interval", type=float, help="reporting interval in seconds", default=60.0
        )

    def main_loop(self) -> None:
        assert self.args
        seconds = self.args.interval

        es = self.elasticsearch_client()

        while True:
            try:
                # limit to columns of interest?
                indices = cast(
                    List[Dict[str, str]],
                    es.cat.indices(bytes="mb", pri=True, format="json"),
                )
                health: Counter[str] = Counter()
                for index in indices:
                    name = index["index"]
                    logger.debug("index: %s", name)

                    def by_index(input: str, out: str) -> None:
                        val = int(index[input])
                        logger.debug(" %s %s %d", input, out, val)
                        self.gauge("indices." + out, val, labels=[("index", name)])

                    by_index("docs.count", "docs")
                    by_index("docs.deleted", "deleted")
                    by_index("pri.store.size", "pri-size")
                    health[index["health"]] += 1

                # report totals for each health state
                for color in ("green", "red", "yellow"):
                    count = health[color]
                    logger.debug("%s %d", color, count)
                    self.gauge("indices.health", count, labels=[("color", color)])

            except (ConnectionError, ConnectionTimeout) as e:
                logger.debug("indices: %r", e)

            # sleep until top of next period:
            sleep_sec = seconds - time.time() % seconds
            logger.debug("sleep %.6g", sleep_sec)
            time.sleep(sleep_sec)


if __name__ == "__main__":
    app = ElasticStats("elastic-stats", "Send Elastic Search stats to statsd")
    app.main()
