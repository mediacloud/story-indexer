"""
report Elastic Search stats to statsd
"""

# Phil, from rabbitmq-stats.py
# with help from importer

from collections import Counter
from logging import getLogger
from typing import Any, Dict, cast

from elastic_transport import ConnectionError, ConnectionTimeout

from indexer.app import App, IntervalMixin
from indexer.elastic import ElasticMixin

logger = getLogger("elastic-stats")

StatsDict = dict[str, dict[str, int | float | dict[str, int | float]]]


class ElasticStats(ElasticMixin, IntervalMixin, App):
    def index(self, name: str, data: StatsDict) -> None:
        # just dump it all for now, rather than trying to figure out what's useful
        pri = data["primaries"]  # vs "total"
        for k1, v1 in pri.items():
            if isinstance(v1, (int, float)):
                path = f"{name}.primaries.{k1}"
                logger.debug(" %s %s", path, v1)
                self.gauge(path, v1)
            elif isinstance(v1, dict):
                for k2, v2 in v1.items():
                    if isinstance(v2, (int, float)):
                        # NOTE! bool is subclass of int!!!
                        path = f"{name}.primaries.{k1}.{k2}"
                        logger.debug(" %s %s", path, v2)
                        self.gauge(path, v2)

    def main_loop(self) -> None:
        while True:
            try:
                es = self.elasticsearch_client()

                # see https://github.com/mediacloud/story-indexer/issues/199
                stats = cast(Dict[str, Any], es.indices.stats())  # fetches /_stats

                # top level keys: "_shards", "_all", "indices"
                all = stats["_all"]
                self.index("all", all)

                # when started using ILM, no longer reported individual index stats.
                # with kibana needed to again (unless/until hidden)
                health: Counter[str] = Counter()

                for name, values in stats["indices"].items():
                    if name[0] == ".":  # hide monitoring indices
                        continue
                    health[values["health"]] += 1  # sum by color
                    self.index("indices." + name, values)

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
