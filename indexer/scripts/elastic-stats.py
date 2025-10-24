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

LabelsType = list[tuple[str, str]]


class ElasticStats(ElasticMixin, IntervalMixin, App):
    def g(self, name: str, value: int | float, labels: LabelsType = []) -> None:
        # since stats are the purpose of this program, log them!
        logger.debug("%s %s %s", name, value, labels)
        self.gauge(name, value, labels)

    def index(self, name: str, data: StatsDict, labels: LabelsType = []) -> None:
        # just dump it all for now, rather than trying to figure out what's useful
        pri = data["primaries"]  # vs "total"
        for k1, v1 in pri.items():
            if isinstance(v1, (int, float)):
                self.g(f"{name}.primaries.{k1}", v1, labels=labels)
            elif isinstance(v1, dict):
                for k2, v2 in v1.items():
                    # NOTE! bool is subclass of int!!!
                    if isinstance(v2, (int, float)):
                        self.g(f"{name}.primaries.{k1}.{k2}", v2, labels=labels)

    def indices_stats(self) -> None:
        es = self.elasticsearch_client()
        # see https://github.com/mediacloud/story-indexer/issues/199
        stats = cast(Dict[str, Any], es.indices.stats())  # fetches /_stats

        # top level keys: "_shards", "_all", "indices"
        all = stats["_all"]
        self.index("indices.all", all)

        # when started using ILM, no longer reported individual index stats.
        # with kibana needed to again (unless/until hidden)
        ihealth: Counter[str] = Counter()

        for index_name, values in stats["indices"].items():
            # hide monitoring/ILM/snapshotting indices
            if index_name[0] == ".":
                continue
            ihealth[values["health"]] += 1  # sum by color
            self.index("indices.indices", values, labels=[("name", index_name)])

        # report totals for each health state
        for color in ("green", "red", "yellow"):
            count = ihealth[color]
            self.g("indices.health", count, labels=[("color", color)])

    def node_stats(self) -> None:
        es = self.elasticsearch_client()
        stats = cast(Dict[str, Any], es.nodes.stats().raw)

        for node_id, node_data in stats["nodes"].items():
            node_name = node_data["name"].split(".")[0]
            node_labels: LabelsType = [("node", node_name)]

            jvm_data = node_data["jvm"]
            self.g(
                "node.jvm.mem.heap_used_percent",
                jvm_data["mem"]["heap_used_percent"],
                labels=node_labels,
            )

            # sum data across all garbage collecors
            gc_count = 0
            gc_millis = 0
            for collector in jvm_data["gc"]["collectors"].values():
                gc_count += collector["collection_count"]
                gc_millis += collector["collection_time_in_millis"]
            self.g("node.jvm.gc.collection_count", gc_count, labels=node_labels)
            self.g("node.jvm.gc.collection_millis", gc_millis, labels=node_labels)

            self.g(
                "node.http.client_count",
                len(node_data["http"]["clients"]),
                labels=node_labels,
            )

            for pool_name, pool_data in node_data["thread_pool"].items():
                pool_label = node_labels + [("pool", pool_name)]
                # these _look_ like they might be summable, but seems unlikely
                for attr in ["queue", "active", "rejected", "completed"]:
                    self.g(
                        f"node.thread_pool.{attr}", pool_data[attr], labels=pool_label
                    )

            # description of "parent" breaker:
            # https://opster.com/analysis/elasticsearch-updated-breaker-settings-parent/
            for breaker_name, breaker_data in node_data["breakers"].items():
                self.g(
                    "node.breakers.tripped",
                    breaker_data["tripped"],
                    labels=node_labels + [("name", breaker_name)],
                )

            os_data = node_data["os"]
            cpu_data = os_data["cpu"]

            cpu_pct = cpu_data["percent"]
            self.g("node.os.cpu.percent", cpu_pct, labels=node_labels)

            # report in old location too, for now
            # (can be removed after one week in production)
            self.g("cat.nodes.cpu", cpu_pct, labels=node_labels)

            lavg = cpu_data["load_average"]
            for m in (1, 5, 15):
                value = lavg[f"{m}m"]
                self.g(f"node.os.cpu.load_average.{m}m", value, labels=node_labels)

                # report in old location too, for now
                # (can be removed after one week in production)
                self.g(f"cat.nodes.load_{m}m", value, labels=node_labels)

    def cluster_health(self) -> None:
        es = self.elasticsearch_client()
        cluster_health = cast(Dict[str, Any], es.cluster.health().raw)

        # not reporing: status, number_of_nodes,  number_of_data_nodes

        for health in ["green", "yellow", "red"]:
            # report 1 for the label with the current status, zero for the rest, for summing
            self.g(
                "cluster.health.status",
                int(health == cluster_health["status"]),
                labels=[("color", health)],
            )

        # ASSuming "active_shards" doesn't include initializing_shards or unassigned_shards,
        # but leaving it out so that any non-zero value can be alerted.
        # DON'T include total shards; labels are meant to able to be summed!!!
        # Also ignoring active_primary_shards.
        for status in [
            "relocating",
            "initializing",
            "unassigned",
            "delayed_unassigned",
        ]:
            self.g(
                "cluster.health.shards",
                cluster_health[status + "_shards"],
                labels=[("status", status)],
            )

        for short, attr in [
            ("pending_tasks", "number_of_pending_tasks"),
            ("task_max_wait_millis", "task_max_waiting_in_queue_millis"),
            ("active_shards_pct", "active_shards_percent_as_number"),
            ("inflight_fetch", "number_of_in_flight_fetch"),
        ]:
            self.g(f"cluster.health.pending_tasks.{short}", cluster_health[attr])

    def snap_stats(self) -> None:
        # NOTE!! assumes just one repository and one policy!!
        es = self.elasticsearch_client()
        j = cast(Dict[str, Any], es.snapshot.get(repository="*", snapshot="*"))
        success = 0
        snaps = j.get("snapshots", [])
        if len(snaps) > 0:
            last = snaps[-1]
            # possible states: IN_PROGRESS, SUCCESS, FAILED, PARTIAL, INCOMPATIBLE
            last_state = last.get("state")
            logger.debug("snap_stats last_state %r", last_state)
            if last_state == "SUCCESS":
                success = 1
        # put state in a label, so possible to add more
        # (would need to .lower() state name and translate "_" to "-")
        self.g("snapshot.last", success, labels=[("state", "success")])

    def main_loop(self) -> None:
        while True:
            try:
                self.indices_stats()
            except (ConnectionError, ConnectionTimeout) as e:
                logger.warning("indices.stats: %r", e)

            try:
                self.node_stats()
            except (ConnectionError, ConnectionTimeout, KeyError) as e:
                logger.warning("nodes.stats: %r", e)

            try:
                self.cluster_health()
            except (ConnectionError, ConnectionTimeout, KeyError) as e:
                logger.warning("cluster.health: %r", e)

            try:
                self.snap_stats()
            except (ConnectionError, ConnectionTimeout, KeyError) as e:
                logger.warning("snap stats: %r", e)

            # sleep until top of next period:
            self.interval_sleep()


if __name__ == "__main__":
    app = ElasticStats("elastic-stats", "Send Elastic Search stats to statsd")
    app.main()
