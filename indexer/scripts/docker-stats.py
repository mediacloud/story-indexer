"""
Docker Stats

Must:
* run on a Swarm manager node
* have host docker unix-domain socket mounted as volume!
"""

import argparse
from logging import getLogger
from typing import Dict

import docker.client  # mypy doesn't see from_env in top module

from indexer.app import App, IntervalMixin

logger = getLogger("docker-stats")


class DockerStats(IntervalMixin, App):
    """
    report docker stats
    """

    def define_options(self, ap: argparse.ArgumentParser) -> None:
        super().define_options(ap)

        ap.add_argument(
            "--stack",
            help="docker stack name (required)",
            required=True,
        )

    def g(
        self,
        input: Dict[str, int],
        input_item: str,
        name: str,
        label: str,
        label_value: str,
    ) -> None:
        """
        report a gauge, with debug; report zero if missing
        """
        value = input.get(input_item, 0)
        logger.debug("%s[%s=%s]: %d", name, label, label_value, value)
        self.gauge(name, value, [(label, label_value)])

    def main_loop(self) -> None:
        assert self.args
        stack = self.args.stack
        client = docker.client.from_env()
        while True:
            for service in client.services.list(status=True):
                stack_name, service_name = service.name.split("_", 1)
                if stack_name != stack:
                    logger.debug(
                        "skipping stack %s service %s", stack_name, service_name
                    )
                    continue
                status = service.attrs.get("ServiceStatus", {})
                for input, output in (
                    ("RunningTasks", "service.running"),
                    ("DesiredTasks", "service.desired"),
                    ("CompletedTasks", "service.completed"),
                ):
                    self.g(status, input, output, "name", service_name)

            # sleep until top of next period:
            self.interval_sleep()


if __name__ == "__main__":
    app = DockerStats("docker-stats", "Send Docker stats to statsd")
    app.main()
