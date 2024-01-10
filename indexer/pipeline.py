"""
indexer.pipeline utility for describing and initializing queues
for a processing pipeline using indexer.worker.QApp/Worker/...

But wait, this file is also a script for configuring
the default pipeline.... It's a floor wax AND a desert topping!

https://www.nbc.com/saturday-night-live/video/shimmer-floor-wax/2721424
"""

# PLB: Maybe this should write out RabbitMQ configuration file
# (instead of configuring queues) so that RabbitMQ comes up configured
# on restart?!!! *BUT* this assumes there is ONE true configuration
# (ie; that any queues for backfill processing are included).

import argparse
import logging
import os
import sys
from typing import Any, Callable, Dict, List, Set, Union, cast

# PyPI
import pika
from pika.exchange_type import ExchangeType

# local:
from indexer.app import run
from indexer.worker import (
    DEFAULT_ROUTING_KEY,
    QApp,
    base_queue_name,
    delay_queue_name,
    fast_queue_name,
    input_queue_name,
    output_exchange_name,
    quarantine_queue_name,
)

COMMANDS: List[str] = []

logger = logging.getLogger(__name__)


class PipelineError(RuntimeError):
    """Pipeline exception"""


class _Process:
    """Virtual base class describing a pipeline process (producer, worker, consumer)"""

    def __init__(
        self,
        pipeline: "Pipeline",
        name: str,
        consumer: bool,
        outputs: "Outputs",
        fast_delay: bool = False,
    ):
        if name in pipeline.procs:
            raise PipelineError(f"process {name} is already defined")

        if fast_delay and not consumer:
            raise PipelineError(f"process {name} fast_delay but not consumer?")

        self.name: str = name
        self.pipeline = pipeline
        self.consumer = consumer
        self.outputs = outputs
        self.fast_delay = fast_delay

        pipeline.procs[name] = self


class _ProducerBase(_Process):
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
        fast_delay: bool = False,
    ):
        super().__init__(pipeline, name, consumer, outputs, fast_delay)
        if len(outputs) == 0:
            raise PipelineError(f"{self.__class__.__name__} {name} has no outputs")

        for output in outputs:
            if not output.consumer:
                raise PipelineError(f"{name} output {output.name} not a consumer!")

        self.outputs = outputs


class Producer(_ProducerBase):
    """Process with outputs only"""

    def __init__(self, pipeline: "Pipeline", name: str, outputs: "Outputs"):
        super().__init__(pipeline, name, False, outputs)


class Worker(_ProducerBase):
    """Process with inputs and output"""

    def __init__(
        self,
        pipeline: "Pipeline",
        name: str,
        outputs: "Outputs",
        fast_delay: bool = False,
    ):
        super().__init__(pipeline, name, True, outputs, fast_delay)


class Consumer(_Process):
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
    PIPE_TYPES: List[str]
    DEFAULT_TYPE: str

    PIKA_LOG_DEFAULT = logging.WARNING
    WAIT_FOR_QUEUE_CONFIGURATION = False

    def __init__(self, name: str, descr: str):
        self.procs: Dict[str, _Process] = {}
        super().__init__(name, descr)

    #### QApp methods
    def define_options(self, ap: argparse.ArgumentParser) -> None:
        super().define_options(ap)
        ap.add_argument("command", nargs=1, type=str, choices=COMMANDS, help="Command")
        ap.add_argument(
            "--type",
            type=str,
            choices=self.PIPE_TYPES,
            help=f"pipeline type (default: {self.DEFAULT_TYPE})",
            default=self.DEFAULT_TYPE,
        )

    def lay_pipe(self) -> None:
        """
        override this to define pipeline topology
        """
        raise NotImplementedError("lay_pipe not overridden")

    def main_loop(self) -> None:
        """Not a loop!"""
        assert self.args
        cmds = self.args.command
        assert cmds

        self.lay_pipe()

        cmd = cmds[0]
        self.get_command_func(cmd)()

    #### User methods for defining topology before calling main
    def add_producer(self, name: str, outputs: Outputs) -> Producer:
        """Add a Process with no inputs"""
        return Producer(self, name, outputs)

    def add_worker(
        self, name: str, outputs: Outputs, fast_delay: bool = False
    ) -> Worker:
        return Worker(self, name, outputs, fast_delay)

    def add_consumer(self, name: str) -> Consumer:
        """Add a Process with no output"""
        return Consumer(self, name)

    def _configure(self, create: bool) -> None:
        """
        create queues etc if create is True; remove if False.
        (so only one routine to modify)
        """

        assert self.connection
        chan = self.connection.channel()

        # nested helper functions to avoid need to pass api, chan, create:

        def queue(qname: str, delay: bool = False) -> None:
            if create:
                logger.debug(f"creating queue {qname}")

                # durable means queue survives reboot,
                # NOT default delivery mode!
                # see https://www.rabbitmq.com/queues.html#durability

                arguments = {}

                if delay:
                    # policy settings preferable to creating queue
                    # with x-... params which cannot be changed after
                    # queue creation, BUT unlikely to change, and RMQ
                    # documentation makes it unclear whether policies
                    # are (or can be made) durable.  An answer might
                    # be to have this code generate a config file for
                    # RabbitMQ....

                    # Use default exchange to route msgs back to input
                    # queue after TTL expires.  TTL set via
                    # "expiration" message property in Worker._retry
                    arguments["x-dead-letter-exchange"] = ""  # default exchange
                    arguments["x-dead-letter-routing-key"] = input_queue_name(
                        base_queue_name(qname)
                    )

                # XXX if using cluster, use quorum queues, setting:
                # arguments["x-queue-type"] = "quorum"

                chan.queue_declare(qname, durable=True, arguments=arguments)
            else:
                logger.debug(f"deleting queue {qname}")
                chan.queue_delete(qname)

        def exchange(ename: str) -> None:
            if create:
                logger.debug(f"creating {etype} exchange {ename}")
                chan.exchange_declare(ename, etype, durable=True)
            else:
                logger.debug(f"deleting exchange {ename}")
                chan.exchange_delete(ename)

        def qbind(dest_queue: str, ename: str, routing_key: str) -> None:
            # XXX make routing_key optional?
            if create:
                logger.debug(f" binding queue {dest_queue_name} to exchange {ename}")
                chan.queue_bind(dest_queue_name, ename, routing_key=routing_key)
            else:
                logger.debug(f" unbinding queue {dest_queue_name} to exchange {ename}")
                chan.queue_unbind(dest_queue_name, ename, routing_key=routing_key)

        #### _configure function body:

        if not create:
            self._set_configured(chan, False)  # MUST be first!!!

        # create queues and exchanges
        for name, proc in self.procs.items():
            if proc.consumer:
                queue(input_queue_name(name))
                queue(quarantine_queue_name(name))
                queue(delay_queue_name(name), delay=True)
                if proc.fast_delay:
                    queue(fast_queue_name(name), delay=True)

            if proc.outputs:
                ename = output_exchange_name(proc.name)
                if len(proc.outputs) == 1:
                    etype = ExchangeType.direct
                else:
                    etype = ExchangeType.fanout
                exchange(ename)

                for outproc in proc.outputs:
                    dest_queue_name = input_queue_name(outproc.name)
                    qbind(dest_queue_name, ename, DEFAULT_ROUTING_KEY)

        if create:
            # create semaphore
            self._set_configured(chan, True)  # MUST be last!!!

    #### Commands (in alphabetical order)
    @command
    def configure(self) -> None:
        """configure queues etc."""
        self._configure(True)

    @command
    def configure_and_loop(self) -> None:
        """loop configuring queues etc and sleeping to handle RabbitMQ restart"""
        # this exists to handle the race if this starts
        # before RabbitMQ restarts.
        # NOTE: not handling exceptions, let Docker restart.
        while True:
            if not self._test_configured():
                self._configure(True)
            assert self.connection
            self.connection.sleep(5 * 60)

    @command
    def delete(self) -> None:
        """delete queues etc."""
        self._configure(False)

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

    @command
    def test(self) -> None:
        """test if queues configured"""
        assert self.connection
        if self._test_configured():
            print("configured")
            sys.exit(0)
        else:
            print("not configured")
            sys.exit(1)

    @command
    def wait(self) -> None:
        """wait until queues configured"""
        self.wait_until_configured()

    #### utilities
    def get_command_func(self, cmd: str) -> Callable[[], None]:
        """returns command function as a bound method"""
        meth = getattr(self, cmd)
        assert callable(meth)
        return cast(Callable[[], None], meth)

    def get_definitions(self) -> Dict:
        api = self.admin_api()
        defns = api.get_definitions()
        assert isinstance(defns, dict)
        return defns


class MyPipeline(Pipeline):
    PIPE_TYPES = ["batch-fetcher", "queue-fetcher", "historical", "archive"]
    DEFAULT_TYPE = "batch-fetcher"

    def lay_pipe(self) -> None:
        """
        define pipeline topology
        """
        assert self.args
        pt = self.args.type

        logger.info("pipe type: %s", pt)

        importer = self.add_worker("importer", [self.add_consumer("archiver")])
        # parse/import/archive
        p_i_a = self.add_worker("parser", [importer])

        # create exchanges to route outputs to regular parse/import/archive chain
        # XXX option for queue-rss producer, feeding fetcher as worker!
        if pt == "batch-fetcher":
            self.add_producer("fetcher", [p_i_a])
        elif pt == "queue-fetcher":
            self.add_producer(
                "rss-queuer", [self.add_worker("fetcher", [p_i_a], fast_delay=True)]
            )
        elif pt == "historical":
            self.add_producer("hist-queuer", [self.add_worker("hist-fetcher", [p_i_a])])
            # XXX don't want archiver??
        elif pt == "warc":
            self.add_producer("warc-queuer", [importer])
            # XXX don't want archiver??


if __name__ == "__main__":
    # when invoked via "python -m indexer.pipeline"
    run(MyPipeline, "pipeline", "configure story-indexer queues")
