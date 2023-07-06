"""
indexer.pipeline utility for describing and initializing queues
for a processing pipeline using indexer.worker.QApp/Worker/...

Every consumer has an an input queue WORKERNAME-in, Every producer has
an output exchange WORKERNAME-out with links to downstream input
queues.
"""

import argparse
import sys
from typing import Callable, Dict, List, Set, Union, cast

# local:
from indexer.worker import QApp

COMMANDS: List[str] = []


class PipelineError(RuntimeError):
    """Pipeline exception"""


class Process:
    """Virtual class describing a pipeline process (producer, worker, consumer)"""

    def __init__(self, pipeline: "Pipeline", name: str, producer: bool):
        if name in pipeline.workers:
            raise PipelineError(f"{name} is already defined")

        self.name: str = name
        self.pipeline: "Pipeline" = pipeline
        self.producer: bool = producer

        pipeline.workers[name] = self
        pipeline.queues.add(name)  # XXX f"{name}-in" ?


class Producer(Process):
    """Process with no inputs"""

    def __init__(self, pipeline: "Pipeline", name: str):
        super().__init__(pipeline, name, True)


Inputs = List[Union[Producer, "Worker"]]


class Worker(Process):
    """Process with inputs and output"""

    def __init__(
        self, pipeline: "Pipeline", name: str, inputs: Inputs, _producer: bool = True
    ):
        if len(inputs) == 0:
            raise PipelineError(f"consumer {name} has no inputs")

        for input in inputs:
            if not input.producer:
                raise PipelineError(f"{name} input {input.name} not a producer!")

        super().__init__(pipeline, name, _producer)


class Consumer(Worker):
    """Process with inputs but no output"""

    def __init__(self, pipeline: "Pipeline", name: str, inputs: Inputs):
        super().__init__(pipeline, name, inputs, False)


def command(func: Callable) -> Callable:
    """decorator for Pipeline command methods"""
    COMMANDS.append(func.__name__)
    return func


class Pipeline(QApp):
    def __init__(self, name: str, descr: str):
        self.queues: Set = set()
        self.workers: Dict[str, Process] = {}
        super().__init__(name, descr)

    def add_producer(self, name: str) -> Producer:
        """Add a Process with no inputs"""
        return Producer(self, name)

    def add_worker(self, name: str, inputs: Inputs) -> Worker:
        return Worker(self, name, inputs)

    def add_consumer(self, name: str, inputs: Inputs) -> Consumer:
        """Add a Process with no output"""
        return Consumer(self, name, inputs)

    def define_options(self, ap: argparse.ArgumentParser) -> None:
        super().define_options(ap)
        ap.add_argument("command", nargs=1, type=str, choices=COMMANDS, help="Command")

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

    def get_command_func(self, cmd: str) -> Callable:
        """returns command function as a bound method"""
        return cast(Callable, getattr(self, cmd))

    def main_loop(self) -> None:
        assert self.args
        cmds = self.args.command
        assert cmds

        cmd = cmds[0]
        self.get_command_func(cmd)()


if __name__ == "__main__":
    p = Pipeline("test", "test of pipeline configurator")
    p.add_consumer("c", [p.add_worker("b", [p.add_producer("a")])])

    print(p.queues)
    print(p.workers)
    p.main()
