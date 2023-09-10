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
        # XXX make a command line option?
        # _could_ use 20sec (default graphite recording period)
        seconds = 60

        api = self.admin_api()

        def g(input: Dict[str, Any], item: str, prefix: str) -> None:
            if input and item in input:
                value = input[item]
            else:
                # report everything, even if missing
                value = 0
            path = f"{prefix}.{item}"
            logger.debug("%s: %s", path, value)
            self.gauge(path, value)

        while True:
            # PLB: FEH!  All the bother to make AdminMixin, and the URL I want isn't included!
            # Looks like the core of AdminAPI doesn't do pagination either!!!
            queues = api._api_get("/api/queues")
            # returns List[Dict[str,Any]]
            for q in queues:
                prefix = f"queues.{q['name']}"

                g(q, "memory", prefix)
                g(q, "messages_ready", prefix)
                g(q, "messages_unacknowledged", prefix)

                ms = q.get("message_stats", None)
                g(ms, "ack", prefix)
                g(ms, "deliver", prefix)
                g(ms, "publish", prefix)
                g(ms, "redeliver", prefix)
                g(ms, "consumers", prefix)

            nodes = api.list_nodes()
            if nodes:
                n0 = nodes[0]
            else:
                n0 = {}
            prefix = "node"
            g(n0, "fd_used", prefix)
            g(n0, "mem_used", prefix)
            g(n0, "sockets_used", prefix)

            # exchange: unroutable?? (need to skip SEMAPHORE!)
            # exchanges = api.list_exchanges()
            # List w/ {"name": "foo", "message_stats": { ... }}

            # sleep until top of next period:
            sleep_sec = seconds - time.time() % seconds
            logger.debug("sleep %.6g", sleep_sec)
            time.sleep(sleep_sec)


if __name__ == "__main__":
    app = QStats("rabbitmq-stats", "Send RabbitMQ stats to statsd")
    app.main()
