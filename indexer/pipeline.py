"""
indexer.pipeline utility for describing and initializing queues
for a processing pipeline using indexer.worker.QApp/Worker/...

But wait, this file is also a script for configuring
the default pipeline.... It's a floor wax AND a desert topping!

https://www.nbc.com/saturday-night-live/video/shimmer-floor-wax/2721424
"""

import argparse
import logging
import sys
from typing import Any, Callable, Dict, List, Set, Union, cast

# PyPI
import pika
from pika.exchange_type import ExchangeType
from rabbitmq_admin import AdminAPI

# local:
from indexer.worker import (
    DEFAULT_ROUTING_KEY,
    QApp,
    input_queue_name,
    output_exchange_name,
)

COMMANDS: List[str] = []

logger = logging.getLogger(__name__)


class PipelineError(RuntimeError):
    """Pipeline exception"""


class Process:
    """Virtual base class describing a pipeline process (producer, worker, consumer)"""

    def __init__(
        self, pipeline: "Pipeline", name: str, consumer: bool, outputs: "Outputs"
    ):
        if name in pipeline.procs:
            raise PipelineError(f"process {name} is already defined")

        self.name: str = name
        self.pipeline: "Pipeline" = pipeline
        self.consumer: bool = consumer
        self.outputs = outputs

        pipeline.procs[name] = self


class ProducerBase(Process):
    """
    Base class for Process with outputs.
    NOT meant for direct use!
    """

    def __init__(
        self,
        pipeline: "Pipeline",
        name: str,
        consumer: bool,
        outputs: "Outputs",
    ):
        super().__init__(pipeline, name, consumer, outputs)
        if len(outputs) == 0:
            raise PipelineError(f"{self.__class__.__name__} {name} has no outputs")

        for output in outputs:
            if not output.consumer:
                raise PipelineError(f"{name} output {output.name} not a consumer!")

        self.outputs = outputs


class Producer(ProducerBase):
    """Process with outputs only"""

    def __init__(self, pipeline: "Pipeline", name: str, outputs: "Outputs"):
        super().__init__(pipeline, name, False, outputs)


class Worker(ProducerBase):
    """Process with inputs and output"""

    def __init__(self, pipeline: "Pipeline", name: str, outputs: "Outputs"):
        super().__init__(pipeline, name, True, outputs)


class Consumer(Process):
    """Process with inputs but no output"""

    def __init__(self, pipeline: "Pipeline", name: str):
        super().__init__(pipeline, name, True, [])


Outputs = List[Union[Worker, Consumer]]

# CommandMethod = Callable[["Pipeline"],None]
CommandMethod = Callable


def command(func: CommandMethod) -> CommandMethod:
    """decorator for Pipeline command methods"""
    COMMANDS.append(func.__name__)
    return func


class Pipeline(QApp):
    PIKA_LOG_DEFAULT = logging.WARNING

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
        assert self.connection
        chan = self.connection.channel()

        # create queues and exchanges
        for name, proc in self.procs.items():
            if proc.consumer:
                qname = input_queue_name(name)
                # durable == messages stored on disk
                logger.debug(f"declaring queue {qname}")
                chan.queue_declare(qname, durable=True)

            if proc.outputs:
                ename = output_exchange_name(proc.name)
                if len(proc.outputs) == 1:
                    etype = ExchangeType.direct
                else:
                    etype = ExchangeType.fanout
                logger.debug(f"declaring {etype} exchange {ename}")
                chan.exchange_declare(ename, etype)

                for outproc in proc.outputs:
                    dest_queue_name = input_queue_name(outproc.name)
                    logger.debug(
                        f" binding queue {dest_queue_name} to exchange {ename}"
                    )
                    chan.queue_bind(
                        dest_queue_name, ename, routing_key=DEFAULT_ROUTING_KEY
                    )

    @command
    def delete(self) -> None:
        """delete queues"""
        assert self.connection
        chan = self.connection.channel()

        for name, proc in self.procs.items():
            if proc.consumer:
                qname = input_queue_name(name)
                logger.debug(f"deleting queue {qname}")
                chan.queue_delete(qname)

            if proc.outputs:
                ename = output_exchange_name(name)
                logger.debug(f"deleting exchange {ename}")
                chan.exchange_delete(ename)

                for outproc in proc.outputs:
                    dest_queue_name = input_queue_name(outproc.name)
                    logger.debug(
                        f" deleting binding {dest_queue_name} queue to exchange {ename}"
                    )
                    chan.queue_unbind(
                        dest_queue_name, ename, routing_key=DEFAULT_ROUTING_KEY
                    )

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
        for what in ("queues", "exchanges", "bindings"):
            things = defns[what]
            print("")
            if things:
                print(what)
                for thing in things:
                    print("   ", thing)
            else:
                print(f"no {what}")

    #### utilities
    def get_command_func(self, cmd: str) -> Callable[[], None]:
        """returns command function as a bound method"""
        meth = getattr(self, cmd)
        assert callable(meth)
        return cast(Callable[[], None], meth)

    def get_definitions(self) -> Dict:
        """
        use rabbitmq_admin package to get server config via RabbitMQ admin API.
        Uses pika (AMQP) parsed URL for connection params
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
    # when invoked via "python -m indexer.pipeline"
    # or "python indexer/pipeline.py"
    # configure the default queues.
    p = Pipeline("pipeline", "configure story-indexer queues")
    p.add_producer("fetcher", [p.add_worker("parser", [p.add_consumer("importer")])])
    p.main()
