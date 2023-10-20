"""
Report RabbitMQ stats
"""

# Phil, with logic from
# https://github.com/lahaxearnaud/rabbitmq-statsd
# which uses
# https://github.com/KristjanTammekivi/rabbitmq-admin

import argparse
import time
from logging import getLogger
from socket import gaierror  # DNS errors
from typing import Any, Dict

# PyPI
from requests.exceptions import ConnectionError

from indexer.app import IntervalMixin
from indexer.worker import QApp

logger = getLogger("rabbitmq-stats")


class QStats(IntervalMixin, QApp):
    """
    monitor RabbitMQ via AdminAPI
    """

    AUTO_CONNECT = False  # never connects (uses AdminAPI)!

    def g(
        self,
        input: Dict[str, Any],
        input_item: str,
        prefix: str,
        output_item: str,
        label: str,
        label_value: str,
    ) -> None:
        """
        shortcut for reporting a labeled gauge
        report zeros if no values available
        """

        if input:
            value = input.get(input_item, 0)
        else:
            value = 0

        output = f"{prefix}.{output_item}"
        logger.debug("%s %s=%s: %s", output, label, label_value, value)
        self.gauge(output, value, [(label, label_value)])

    def main_loop(self) -> None:
        api = self.admin_api()

        while True:
            try:
                # PLB: FEH! # primary URL I want isn't included in API!
                # Looks like the core of AdminAPI doesn't do pagination either!!!

                queues = api._api_get("/api/queues")
                # returns List[Dict[str,Any]]

                for q in queues:
                    name = q.get("name")
                    self.g(q, "memory", "queues", "mem", "name", name)
                    self.g(q, "messages_ready", "queues", "ready", "name", name)
                    self.g(
                        q, "messages_unacknowledged", "queues", "unacked", "name", name
                    )

                    ms = q.get("message_stats", None)
                    self.g(ms, "ack", "queues", "ack", "name", name)
                    self.g(ms, "deliver", "queues", "deliver", "name", name)
                    self.g(ms, "publish", "queues", "publish", "name", name)
                    self.g(ms, "redeliver", "queues", "redeliver", "name", name)
                    self.g(ms, "consumers", "queues", "consumers", "name", name)

                nodes = api.list_nodes()
                for node in nodes:  # List
                    # leading part is cluster name, split and give second part??
                    name = node.get("name").replace("@", "-")
                    self.g(node, "fd_used", "nodes", "fds", "name", name)
                    self.g(node, "mem_used", "nodes", "memory", "name", name)
                    self.g(node, "sockets_used", "nodes", "sockets", "name", name)

                # exchange: unroutable?? (need to skip SEMAPHORE!)
                # exchanges = api.list_exchanges()
                # List w/ {"name": "foo", "message_stats": { ... }}
            except (ConnectionError, gaierror) as e:
                logger.debug("caught %r", e)

            # sleep until top of next period:
            self.interval_sleep()


if __name__ == "__main__":
    app = QStats("rabbitmq-stats", "Send RabbitMQ stats to statsd")
    app.main()
