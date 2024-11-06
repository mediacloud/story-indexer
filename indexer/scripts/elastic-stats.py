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

    def indices_stats(self) -> None:
        es = self.elasticsearch_client()
        # see https://github.com/mediacloud/story-indexer/issues/199
        stats = cast(Dict[str, Any], es.indices.stats())  # fetches /_stats

        # top level keys: "_shards", "_all", "indices"
        all = stats["_all"]
        self.index("all", all)

        # when started using ILM, no longer reported individual index stats.
        # with kibana needed to again (unless/until hidden)
        ihealth: Counter[str] = Counter()

        for name, values in stats["indices"].items():
            if name[0] == ".":  # hide monitoring indices
                continue
            ihealth[values["health"]] += 1  # sum by color
            self.index("indices." + name, values)

        # report totals for each health state
        for color in ("green", "red", "yellow"):
            count = ihealth[color]
            logger.debug("indices.health %s %d", color, count)
            self.gauge("indices.health", count, labels=[("color", color)])

    def node_stats(self) -> None:
        es = self.elasticsearch_client()
        stats = cast(Dict[str, Any], es.nodes.stats().raw)

        for node_id, node_data in stats["nodes"].items():
            node_name = node_data["name"].split(".")[0]

            def ng(ni: list[str], value: int) -> None:
                """
                per-node gauge
                """
                gname = "nodes." + ".".join(ni)
                logger.debug("%s %d %s", gname, value, node_name)
                self.gauge(gname, value, labels=[("node", node_name)])

            # sum data across all garbage collecors
            jvm_data = node_data["jvm"]
            ng(
                ["jvm.mem.heap_used_percent"],
                jvm_data["mem"]["heap_used_percent"],
            )
            gc_count = 0
            gc_millis = 0
            for collector in jvm_data["gc"]["collectors"].values():
                gc_count += collector["collection_count"]
                gc_millis += collector["collection_time_in_millis"]
            ng(["jvm.gc.collection_count"], gc_count)
            ng(["jvm.gc.collection_millis"], gc_millis)

            ng(["http.client_count"], len(node_data["http"]["clients"]))

            for pool_name, pool_data in node_data["thread_pool"].items():
                for attr in ["queue", "active", "rejected", "completed"]:
                    ng(["thread_pool", pool_name, attr], pool_data[attr])

            # description of "parent" breaker:
            # https://opster.com/analysis/elasticsearch-updated-breaker-settings-parent/
            for breaker_name, breaker_data in node_data["breakers"].items():
                ng(
                    ["breakers", breaker_name, "tripped"],
                    breaker_data["tripped"],
                )

    def cluster_health(self) -> None:
        es = self.elasticsearch_client()
        cluster_health = cast(Dict[str, Any], es.cluster.health().raw)

        def chg(name: str, value: int, labels: list[tuple[str, str]] = []) -> None:
            fname = "cluster.health." + name
            logger.debug("%s %s %s", fname, value, labels)
            self.gauge(fname, value, labels)

        for health in ["green", "yellow", "red"]:
            # report 1 for the label with the current status, zero for the rest
            chg(
                "status",
                int(health == cluster_health["status"]),
                labels=[("color", health)],
            )

        # currently inoring: timed_out,  number_of_data_nodes

        # ASSuming active_shards doesn't include initializing_shards or unassigned_shards,
        # but leaving it out so that any non-zero value can be alerted.
        for status in ["initializing", "unassigned", "delayed_unassigned"]:
            chg(
                "shards",
                cluster_health[status + "_shards"],
                labels=[("status", status)],
            )

        chg("pending_tasks", cluster_health["number_of_pending_tasks"])
        # ignoring number_of_in_flight_fetch, task_max_waiting_in_queue_millis, active_shards_percent_as_number

    def main_loop(self) -> None:
        while True:
            try:
                self.indices_stats()
            except (ConnectionError, ConnectionTimeout) as e:
                logger.debug("indices.stats: %r", e)

            try:
                self.node_stats()
            except (ConnectionError, ConnectionTimeout, KeyError) as e:
                logger.debug("nodes.stats: %r", e)

            try:
                self.cluster_health()
            except (ConnectionError, ConnectionTimeout, KeyError) as e:
                logger.debug("cluster.health: %r", e)

            # sleep until top of next period:
            self.interval_sleep()


if __name__ == "__main__":
    app = ElasticStats("elastic-stats", "Send Elastic Search stats to statsd")
    app.main()
