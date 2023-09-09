"""
Report RabbitMQ stats
"""

# Phil, with logic from
# https://github.com/lahaxearnaud/rabbitmq-statsd
# which uses
# https://github.com/KristjanTammekivi/rabbitmq-admin

import time
from logging import getLogger
from typing import Any, Dict

from indexer.worker import QApp

logger = getLogger("rabbitmq-stats")


class QStats(QApp):
    """
    monitor RabbitMQ via AdminAPI
    """

    AUTO_CONNECT = False  # never connects (uses AdminAPI)!

    def main_loop(self) -> None:
        seconds = 60  # XXX make a command line option?
        api = self.admin_api()
        while True:
            # PLB: FEH!  All the bother to make AdminMixin, and the URL I want isn't included!
            # Looks like the core of AdminAPI doesn't do pagination either!!!
            queues = api._api_get("/api/queues")
            # returns List[Dict[str,Any]]
            for q in queues:
                prefix = f"queues.{q['name']}"

                def g(input: Dict[str, Any], item: str) -> None:
                    if input and item in input:
                        value = input[item]
                    else:
                        # report everything, even if missing
                        value = 0
                    path = f"{prefix}.{item}"
                    logger.debug("%s: %s", path, value)
                    self.gauge(path, value)

                g(q, "memory")
                g(q, "messages_ready")
                g(q, "messages_unacknowledged")

                ms = q.get("message_stats", None)
                g(ms, "ack")
                g(ms, "deliver")
                g(ms, "publish")
                g(ms, "redeliver")
                g(ms, "consumers")

            # MORE???
            # https://www.datadoghq.com/blog/rabbitmq-monitoring/
            # exchange: unroutable??
            # node: fds, sockets, disk used, memory used

            # nodes = api.list_nodes()

            # sleep until top of next period:
            sleep_sec = seconds - time.time() % seconds
            logger.debug("sleep %.6g", sleep_sec)
            time.sleep(sleep_sec)


if __name__ == "__main__":
    app = QStats("rabbitmq-stats", "Send RabbitMQ stats to statsd")
    app.main()
