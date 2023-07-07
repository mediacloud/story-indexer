"""
indexer.pipeline utility for describing and initializing queues
for a processing pipeline using indexer.worker.QApp/Worker/...

Every consumer has an an input queue WORKERNAME-in. Every producer has
an output exchange WORKERNAME-out with links to downstream input
queues, a worker is a process with both inputs and outputs.
"""

import argparse
import sys
from typing import Any, Callable, Dict, List, Set, Union, cast

# PyPI
import pika
from rabbitmq_admin import AdminAPI

# local:
from indexer.worker import QApp

COMMANDS: List[str] = []


class PipelineError(RuntimeError):
    """Pipeline exception"""


class Process:
    """Virtual base class describing a pipeline process (producer, worker, consumer)"""

    def __init__(self, pipeline: "Pipeline", name: str, consumer: bool):
        if name in pipeline.procs:
            raise PipelineError(f"process {name} is already defined")

        self.name: str = name
        self.pipeline: "Pipeline" = pipeline
        self.consumer: bool = consumer

        pipeline.procs[name] = self


class ProducerBase(Process):
    """Base class for Process with outputs"""

    def __init__(
        self,
        pipeline: "Pipeline",
        name: str,
        outputs: "Outputs",
        consumer: bool = False,
    ):
        super().__init__(pipeline, name, consumer)
        if len(outputs) == 0:
            raise PipelineError(f"{self.__class__.__name__} {name} has no outputs")

        for output in outputs:
            if not output.consumer:
                raise PipelineError(f"{name} output {output.name} not a consumer!")

        self.outputs = outputs


class Producer(ProducerBase):
    """Process with outputs only"""

    def __init__(self, pipeline: "Pipeline", name: str, outputs: "Outputs"):
        super().__init__(pipeline, name, outputs, False)


class Worker(ProducerBase):
    """Process with inputs and output"""

    def __init__(self, pipeline: "Pipeline", name: str, outputs: "Outputs"):
        super().__init__(pipeline, name, outputs, True)


class Consumer(Process):
    """Process with inputs but no output"""

    def __init__(self, pipeline: "Pipeline", name: str):
        super().__init__(pipeline, name, True)


Outputs = List[Union[Worker, Consumer]]


def command(func: Callable) -> Callable:
    """decorator for Pipeline command methods"""
    COMMANDS.append(func.__name__)
    return func


class Pipeline(QApp):
    def __init__(self, name: str, descr: str):
        self.procs: Dict[str, Process] = {}
        super().__init__(name, descr)

    #### QApp methods
    def define_options(self, ap: argparse.ArgumentParser) -> None:
        super().define_options(ap)
        ap.add_argument("command", nargs=1, type=str, choices=COMMANDS, help="Command")

    def main_loop(self) -> None:
        """Not a loop!"""
        assert self.args
        cmds = self.args.command
        assert cmds

        cmd = cmds[0]
        self.get_command_func(cmd)()

    #### User methods for defining topology before calling main
    def add_producer(self, name: str, outputs: Outputs) -> Producer:
        """Add a Process with no inputs"""
        return Producer(self, name, outputs)

    def add_worker(self, name: str, outputs: Outputs) -> Worker:
        return Worker(self, name, outputs)

    def add_consumer(self, name: str) -> Consumer:
        """Add a Process with no output"""
        return Consumer(self, name)

    #### Commands (in alphabetical order)
    @command
    def configure(self) -> None:
        """configure queues"""
        print("HERE: configure")

    @command
    def delete(self) -> None:
        """delete queues"""
        print("HERE: delete")

    @command
    def help(self) -> None:
        """give this output"""
        # XXX maybe append this to --help output?
        print("Commands (use --help for options):")
        for cmd in COMMANDS:
            descr = self.get_command_func(cmd).__doc__
            print(f"{cmd:12.12} {descr}")

    @command
    def show(self) -> None:
        """show queues"""
        defns = self.get_definitions()
        curr_queues = [q["name"] for q in defns["queues"]]
        curr_exchanges = [(e["name"], e["type"]) for e in defns["exchanges"]]
        print("RabbitMQ current:")
        print("    queues", curr_queues)
        print("    exchanges", curr_exchanges)
        print("    bindings", defns["bindings"])

    #### utilities
    def get_command_func(self, cmd: str) -> Callable:
        """returns command function as a bound method"""
        meth = getattr(self, cmd)
        assert callable(meth)
        return cast(Callable, meth)

    def get_definitions(self) -> Dict:
        """
        use rabbitmq_admin package to get server config via RabbitMQ admin API.
        takes pika (AMQP) parsed URL for connection params
        """
        args = self.args
        assert args
        par = pika.connection.URLParameters(args.amqp_url)
        creds = par.credentials
        assert isinstance(creds, pika.credentials.PlainCredentials)
        port = 15672  # XXX par.port + 10000???
        api = AdminAPI(
            url=f"http://{par.host}:{port}", auth=(creds.username, creds.password)
        )
        defns = api.get_definitions()
        assert isinstance(defns, dict)
        return defns


if __name__ == "__main__":
    p = Pipeline("test", "test of pipeline configurator")
    p.add_producer("a", [p.add_worker("b", [p.add_consumer("c")])])

    for name, proc in p.procs.items():
        print(name, proc)

    p.main()
