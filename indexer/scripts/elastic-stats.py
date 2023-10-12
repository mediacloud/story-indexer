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

from indexer.app import App, IntervalMixin
from indexer.elastic import ElasticMixin

# Reporting unit for storage.
# Changing this will cause a discontinuity in graphs!!!
# But here for clarity, and for reference in comments!
BYTES = "mb"

logger = getLogger("elastic-stats")


class ElasticStats(ElasticMixin, IntervalMixin, App):
    def main_loop(self) -> None:
        es = self.elasticsearch_client()

        while True:
            try:
                # limit to columns of interest?
                indices = cast(
                    List[Dict[str, str]],
                    es.cat.indices(bytes=BYTES, pri=True, format="json"),
                )
                health: Counter[str] = Counter()
                for index in indices:
                    name = index["index"]
                    logger.debug("index: %s", name)

                    def by_index(input: str, out: str) -> None:
                        val = int(index[input])
                        logger.debug(" %s %s %d", input, out, val)
                        self.gauge(
                            f"indices.stats.{out}", val, labels=[("index", name)]
                        )

                    by_index("docs.count", "docs")
                    by_index("docs.deleted", "deleted")
                    by_index("pri.store.size", "pri-size")  # in BYTES
                    health[index["health"]] += 1

                # report totals for each health state
                for color in ("green", "red", "yellow"):
                    count = health[color]
                    logger.debug("%s %d", color, count)
                    self.gauge("indices.health", count, labels=[("color", color)])

            except (ConnectionError, ConnectionTimeout) as e:
                logger.debug("indices: %r", e)

            # sleep until top of next period:
            self.interval_sleep()


if __name__ == "__main__":
    app = ElasticStats("elastic-stats", "Send Elastic Search stats to statsd")
    app.main()
