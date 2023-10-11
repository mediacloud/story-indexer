"""
RabbitMQ Queue Utility program

python -mindexer.scripts.qutil COMMAND QUEUE
"""

# Phil 2023-09-27, with code from pipeline.py

import argparse
import json
import sys
from logging import getLogger
from typing import Any, Callable, Dict, List, Optional, cast

# PyPI
from pika import BasicProperties
from pika.adapters.blocking_connection import BlockingChannel, BlockingConnection
from pika.spec import Basic, Queue

from indexer.worker import QApp

logger = getLogger("qutil")

COMMANDS: List[str] = []

# CommandMethod = Callable[["Pipeline"],None]
CommandMethod = Callable


def command(func: CommandMethod) -> CommandMethod:
    """decorator for QUtil command methods"""
    COMMANDS.append(func.__name__)
    return func


class QUtil(QApp):
    def define_options(self, ap: argparse.ArgumentParser) -> None:
        super().define_options(ap)
        ap.add_argument("--max", type=int, help="max items to process")

        ap.add_argument(
            "command",
            type=str,
            choices=COMMANDS,
            nargs="?",
            default="help",
            help="Command",
        )
        ap.add_argument("queue", type=str, nargs="?", help="Queue")

    def main_loop(self) -> None:
        assert self.args

        cmd = self.args.command or "help"
        self.get_command_func(cmd)()

    #### commands (in alphabetical order, docstring is help)

    @command
    def dump_bin(self) -> None:
        """dump messages as raw bytes"""

        n = 1

        def writer(body: bytes, tag: int, properties: BasicProperties) -> None:
            nonlocal n

            while True:
                base = f"{n:06d}"
                fname = base + ".bin"
                try:
                    # create (eXclusive), binary

                    with open(fname, "xb") as f:
                        f.write(body)
                    logger.info("wrote delivery tag %d as %s", tag, fname)

                    try:
                        pname = base + ".props"
                        # crush existing file
                        with open(pname, "w") as f:
                            json.dump(properties.__dict__, f)
                        logger.info(" wrote properties as %s", pname)
                    except RuntimeError as e:
                        logger.warning(f"{pname}: {e!r}")

                    return
                except FileExistsError:
                    n += 1  # try next number

        self.dump_msgs(writer)

    @command
    def help(self) -> None:
        """give this output"""
        # XXX maybe append this to --help output?
        print("Commands (use --help for options):")
        for cmd in COMMANDS:
            descr = self.get_command_func(cmd).__doc__
            print(f"{cmd:12.12} {descr}")

    @command
    def purge(self) -> None:
        """purge messages from queue"""

        queue = self.get_queue()
        chan = self.get_channel()
        resp = chan.queue_purge(queue)
        rname = resp.NAME
        if rname == Queue.PurgeOk.NAME:
            logger.info("purged %s", queue)
        else:
            logger.warning("purge %s failed: %s", queue, rname)  # XXX detail?

    #### utilities

    def get_channel(self) -> BlockingChannel:
        assert self.connection
        return self.connection.channel()

    def get_command_func(self, cmd: str) -> Callable[[], None]:
        """returns command function as a bound method"""
        meth = getattr(self, cmd)
        assert callable(meth)
        return cast(Callable[[], None], meth)

    def get_queue(self) -> str:
        assert self.args
        queue = self.args.queue
        if queue is None:
            logger.error("need queue")
            sys.exit(1)
        return str(queue)

    def dump_msgs(self, writer: Callable) -> None:
        """
        utility to read messages from queue & call writer
        """
        queue = self.get_queue()
        consumer_tag = ""  # set by basic_consume

        assert self.args

        max = int(self.args.max or sys.maxsize)

        def on_message(
            chan: BlockingChannel,
            method: Basic.Deliver,
            properties: BasicProperties,
            body: bytes,
        ) -> None:
            nonlocal max

            tag = method.delivery_tag
            if tag is None:
                logger.warning("method.delivery_tag is None?")
                return

            writer(body, tag, properties)
            chan.basic_ack(tag)  # discard
            max -= 1
            if max == 0:
                # cancel basic_consume
                chan.basic_cancel(consumer_tag)

        chan = self.get_channel()
        consumer_tag = chan.basic_consume(queue, on_message)
        chan.start_consuming()  # enter pika main loop; calls on_message


if __name__ == "__main__":
    app = QUtil("qutil", "Queue Utility Program")
    app.main()
