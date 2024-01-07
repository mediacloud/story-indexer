"""
RabbitMQ Queue Utility program

python -mindexer.scripts.qutil COMMAND QUEUE [ INPUT_FILE ... ]
"""

# Phil 2023-09-27, with code from pipeline.py

import argparse
import json
import signal
import socket
import sys
from logging import getLogger
from types import FrameType
from typing import Any, Callable, Dict, List, Optional, cast

# PyPI
from pika import BasicProperties
from pika.adapters.blocking_connection import BlockingChannel, BlockingConnection
from pika.spec import Basic, Queue

from indexer.story import BaseStory
from indexer.story_archive_writer import StoryArchiveReader, StoryArchiveWriter
from indexer.storyapp import StorySender
from indexer.worker import DEFAULT_ROUTING_KEY, QApp

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
        ap.add_argument("input_files", type=str, nargs="*", help="input files")

    def main_loop(self) -> None:
        assert self.args

        cmd = self.args.command or "help"
        self.get_command_func(cmd)()

    #### commands (in alphabetical order, docstring is help)

    @command
    def dump_archives(self) -> None:
        """dump messages as archive files"""

        serial = 1
        stories = 0  # stories in current archive
        hostname = socket.gethostname()
        fqdn = socket.getfqdn()
        aw: Optional[StoryArchiveWriter] = None  # archive writer

        def writer(body: bytes, tag: int, properties: BasicProperties) -> None:
            nonlocal aw
            nonlocal serial
            nonlocal stories

            story = BaseStory.load(body)
            if not aw:
                aw = StoryArchiveWriter(
                    prefix=self.get_queue(),
                    hostname=hostname,
                    fqdn=fqdn,
                    serial=serial,
                    work_dir=".",
                )

            # XXX just save headers that start with "x-mc-"?
            extras = {"rabbitmq_headers": properties.headers}
            aw.write_story(story, extra_metadata=extras, raise_errors=False)
            stories += 1

            if stories == 5000:
                aw.finish()
                aw = None
                stories = 0
                serial += 1

        def flusher() -> None:
            if aw:
                aw.finish()

        self.dump_msgs(writer, flusher)

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

        def flusher() -> None:
            return

        self.dump_msgs(writer, flusher)

    @command
    def help(self) -> None:
        """give this output"""
        # XXX maybe append this to --help output?
        print("Commands (use --help for options):")
        for cmd in COMMANDS:
            descr = self.get_command_func(cmd).__doc__
            print(f"{cmd:16.16} {descr}")

    @command
    def load_archives(self) -> None:
        """load archive files into queue"""
        assert self.args

        q = self.get_queue()  # takes command line argument
        if q.endswith("-out"):  # exchange name?
            exchange = q
            routing_key = DEFAULT_ROUTING_KEY
        else:
            exchange = ""  # default exchange
            routing_key = q

        sender = StorySender(self, self.get_channel())

        input_files = self.args.input_files
        if not input_files:
            logger.error("need input files")
            return

        for fname in input_files:
            logger.info("reading archive %s", fname)
            with open(fname, "rb") as f:
                reader = StoryArchiveReader(f)
                count = 0
                for story in reader.read_stories():
                    sender.send_story(story, exchange, routing_key)
                    count += 1
                logger.info("read %d stories", count)

    @command
    def purge(self) -> None:
        """purge messages from queue"""

        queue = self.get_queue()
        chan = self.get_channel()
        resp = chan.queue_purge(queue)
        meth = resp.method
        if meth.NAME == "Queue.PurgeOk":
            logger.info("purged %s OK", queue)
        else:
            logger.warning("purge %s: %r", queue, meth)

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

    def dump_msgs(self, writer: Callable, flush: Callable) -> None:
        """
        utility to read messages from queue & call writer
        """
        queue = self.get_queue()
        consumer_tag = ""  # set by basic_consume

        assert self.args
        max = int(self.args.max or sys.maxsize)

        def renew() -> None:
            signal.alarm(60)

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

            renew()  # keep-alive

            logger.debug("on_message tag %s: %d bytes", tag, len(body))
            writer(body, tag, properties)
            chan.basic_ack(tag)  # discard

            max -= 1
            if max == 0:
                # cancel basic_consume
                chan.basic_cancel(consumer_tag)

        chan = self.get_channel()
        consumer_tag = chan.basic_consume(queue, on_message)

        def on_alarm(signum: int, frame: Optional[FrameType]) -> None:
            logger.info("timeout")
            chan.basic_cancel(consumer_tag)

        signal.signal(signal.SIGALRM, on_alarm)
        renew()  # schedule SIGALRM

        chan.start_consuming()  # enter pika main loop; calls on_message
        flush()  # finish writing any open archive


if __name__ == "__main__":
    app = QUtil("qutil", "Queue Utility Program")
    app.main()
