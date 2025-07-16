"""
RabbitMQ Queue Utility program

python -mindexer.scripts.qutil COMMAND QUEUE [ INPUT_FILE ... ]

NOTE! All sub-commands commands REQUITE a queue name.
For queue lengths, and configuration, use pipeline.py:
        ./run-configure-pipeline.sh -T PIPELINE_TYPE {qlen,show}

NOTE! QApp superclass makes connection to RabbitMQ before processing args
this means you can't even get help response without a valid server URL!
"""

# Phil 2023-09-27, with code from pipeline.py

import argparse
import signal
import socket
import sys
from logging import getLogger
from types import FrameType
from typing import Callable, List, Optional, cast
from urllib.parse import urlparse

# PyPI
from pika import BasicProperties
from pika.adapters.blocking_connection import BlockingChannel
from pika.spec import Basic

from indexer.story import BaseStory
from indexer.story_archive_writer import StoryArchiveReader, StoryArchiveWriter
from indexer.storyapp import RabbitMQStorySender
from indexer.worker import DEFAULT_ROUTING_KEY, QApp

logger = getLogger("qutil")

COMMANDS: List[str] = []

# CommandMethod = Callable[["Pipeline"],None]
CommandMethod = Callable


def command(func: CommandMethod) -> CommandMethod:
    """decorator for QUtil command methods"""
    COMMANDS.append(func.__name__)
    return func


def check_domains(url: Optional[str], domains: List[str]) -> bool:
    """
    return True, if, and only if url hostname is in one of domains
    """
    if not url:
        return False  # not a reason to eliminate

    try:
        u = urlparse(url)
        if not u.hostname:
            return False
    except ValueError:
        return False

    for domain in domains:
        if u.hostname == domain or u.hostname.endswith("." + domain):
            return True
    return False


class QUtil(QApp):
    def define_options(self, ap: argparse.ArgumentParser) -> None:
        super().define_options(ap)
        ap.add_argument("--max", type=int, help="max items to process")

        # NOTE: Only for load_archives: maybe should take flags after the command?
        dgrp = ap.add_mutually_exclusive_group()
        dgrp.add_argument(
            "--ignore-domain",
            help="domain name(s) to NOT load from archives",
            action="append",
        )
        dgrp.add_argument(
            "--only-domain", help="domain name(s) to load exclsively", action="append"
        )
        ap.add_argument(
            "--dry-run",
            help="takes 0/1: required for load_archives",
            type=int,
            default=None,
        )
        # end only for load_archives

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
    def delete(self) -> None:
        """delete queue"""
        q = self.get_queue()  # takes command line argument
        chan = self.get_channel()
        resp = chan.queue_delete(queue=q)
        print("response", resp)

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

            # Dumps EVERYTHING (ArchiveWriter now handles arbitrary data)
            extras = {"rabbitmq_headers": properties.headers}
            aw.write_story(story, extra_metadata=extras, raise_errors=False)
            stories += 1

            # chunk in 5000 story archives as usual: should this be
            # handled by a wrapper class around StoryArchiveWriter?
            if stories == 5000:
                aw.finish()
                aw = None
                stories = 0
                serial += 1

        def flusher() -> None:
            if aw:
                aw.finish()

        self.dump_msgs(writer, flusher)  # honors --max

    @command
    def help(self) -> None:
        """give this output"""
        # XXX maybe append this to --help output?
        print("Commands (use --help for options):")
        for cmd in COMMANDS:
            descr = self.get_command_func(cmd).__doc__
            print(f"{cmd:16.16} {descr}")

    def check_url_domains(self, story: BaseStory) -> bool:
        """
        look for forbidden/desired domains in all the places they might be hiding
        """
        assert self.args

        if domains := self.args.ignore_domain:
            ret = False
        elif domains := self.args.ignore_domain:
            ret = True
        else:
            return True

        for domain in domains:
            # discrete statements (not single expression with and) for debug:
            if check_domains(story.rss_entry().link, domain):
                return ret

            if check_domains(story.http_metadata().final_url, domain):
                return ret

            if check_domains(story.content_metadata().url, self.args.ignore_domain):
                return ret

        return not ret

    @command
    def load_archives(self) -> None:
        """load archive files into queue"""
        assert self.args

        dry_run = self.args.dry_run
        if dry_run not in (0, 1):
            logger.error("need --dry-run={0,1}")
            sys.exit(1)

        # NOTE! Does not check if value queue name!
        q = self.get_queue()  # takes command line argument
        if q.endswith("-out"):  # exchange name?
            exchange = q
            routing_key = DEFAULT_ROUTING_KEY
        else:
            exchange = ""  # default exchange
            routing_key = q

        sender = RabbitMQStorySender(self, self.get_channel())

        input_files = self.args.input_files
        if not input_files:
            logger.error("need input files")
            return

        breakpoint()

        queued = 0
        for fname in input_files:
            logger.info("reading archive %s", fname)
            with open(fname, "rb") as f:
                reader = StoryArchiveReader(f)
                read = passed = 0
                for story in reader.read_stories():
                    read += 1
                    if not self.check_url_domains(story):
                        continue
                    passed += 1  # not dropped
                    if not dry_run:
                        sender.send_story(story, exchange, routing_key)
                        queued += 1
            logger.info("%s: read %d stories, passed %d", fname, read, passed)
        logger.info("total %d stories queued", queued)

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
        """
        pick up queue argument; give error if absent
        does not check that queue exists!
        """
        assert self.args
        queue = self.args.queue
        if queue is None:
            logger.error("need queue")
            sys.exit(1)
        return str(queue)  # typing paranoia

    def dump_msgs(self, writer: Callable, flush: Callable) -> None:
        """
        utility function to read messages from queue & call writer
        """
        queue = self.get_queue()
        consumer_tag = ""  # set by basic_consume

        assert self.args
        max = int(self.args.max or sys.maxsize)

        written = 0

        def renew() -> None:
            signal.alarm(10)

        def on_message(
            chan: BlockingChannel,
            method: Basic.Deliver,
            properties: BasicProperties,
            body: bytes,
        ) -> None:
            nonlocal written

            tag = method.delivery_tag
            if tag is None:
                logger.warning("method.delivery_tag is None?")
                return

            renew()  # keep-alive

            logger.debug("on_message tag %s: %d bytes", tag, len(body))
            writer(body, tag, properties)
            chan.basic_ack(tag)  # discard

            written += 1
            if written == max:
                # cancel basic_consume
                chan.basic_cancel(consumer_tag)
            elif (written % 1000) == 0:
                # give progress reports
                logger.info("%d stories written", written)

        chan = self.get_channel()
        consumer_tag = chan.basic_consume(queue, on_message)

        def on_alarm(signum: int, frame: Optional[FrameType]) -> None:
            logger.info("queue read timeout")
            chan.basic_cancel(consumer_tag)

        signal.signal(signal.SIGALRM, on_alarm)
        renew()  # schedule SIGALRM

        chan.start_consuming()  # enter pika main loop; calls on_message
        flush()  # finish writing any open archive
        logger.info("dumped %d stories", written)


if __name__ == "__main__":
    app = QUtil("qutil", "Queue Utility Program")
    app.main()
