"""
Pipeline Worker Definitions
"""

import argparse
import logging
import os
import pickle
import sys
import time
from typing import Any, List, NamedTuple, Optional, Tuple

# PyPI
from pika import BasicProperties
from pika.adapters.blocking_connection import BlockingChannel, BlockingConnection
from pika.connection import URLParameters
from pika.spec import Basic

# story-indexer
from indexer.app import App, AppException
from indexer.story import BaseStory

logger = logging.getLogger(__name__)

# content types:
MIME_TYPE_PICKLE = "application/python-pickle"

DEFAULT_ROUTING_KEY = "default"


class InputMessage(NamedTuple):
    """used to save batches of input messages"""

    method: Basic.Deliver
    properties: BasicProperties
    body: bytes


def input_queue_name(procname: str) -> str:
    """take process name, return input queue name"""
    # Every consumer has an an input queue NAME-in.
    return procname + "-in"


def output_exchange_name(procname: str) -> str:
    """take process name, return input exchange name"""
    # Every producer has an output exchange NAME-out
    # with links to downstream input queues.
    return procname + "-out"


class QApp(App):
    """
    Base class for AMQP/pika based App.
    Producers (processes that have no input queue)
    should derive from this class
    """

    AUTO_CONNECT = True

    # pika logs (a lot) at INFO level: make logging.WARNING the default?
    # this default can be overridden with "--log-level pika:info"
    PIKA_LOG_DEFAULT: Optional[int] = None

    def __init__(self, process_name: str, descr: str):
        super().__init__(process_name, descr)

        self.connection: Optional[BlockingConnection] = None

        # queues/exchanges created using indexer.pipeline:
        self.input_queue_name = input_queue_name(self.process_name)
        self.output_exchange_name = output_exchange_name(self.process_name)

    def define_options(self, ap: argparse.ArgumentParser) -> None:
        super().define_options(ap)

        # environment variable automagically set in Dokku:
        default_url = os.environ.get("RABBITMQ_URL")  # set by Dokku
        # XXX give env var name instead of value?
        ap.add_argument(
            "--rabbitmq-url",
            "-U",
            dest="amqp_url",
            default=default_url,
            help="override RABBITMQ_URL ({default_url}",
        )

        if self.PIKA_LOG_DEFAULT is not None:
            logging.getLogger("pika").setLevel(self.PIKA_LOG_DEFAULT)

    def process_args(self) -> None:
        super().process_args()

        assert self.args
        if not self.args.amqp_url:
            logger.fatal("need --rabbitmq-url or RABBITMQ_URL")
            sys.exit(1)

        if self.AUTO_CONNECT:
            self.qconnect()

    def qconnect(self) -> None:
        """
        called from process_args if AUTO_CONNECT is True
        """
        assert self.args  # checked in process_args
        url = self.args.amqp_url
        assert url  # checked in process_args
        reconnect_parameters = "/?connection_attempts=10&retry_delay=5"
        self.connection = BlockingConnection(URLParameters(url + reconnect_parameters))
        assert self.connection  # keep mypy quiet
        logger.info(f"connected to {url}")

    def send_message(
        self,
        chan: BlockingChannel,
        data: bytes,
        exchange: Optional[str] = None,
        routing_key: str = DEFAULT_ROUTING_KEY,
    ) -> None:
        chan.basic_publish(exchange or self.output_exchange_name, routing_key, data)


class Worker(QApp):
    """Base class for Workers that consume messages"""

    # XXX maybe allow command line args, environment overrides?
    # override this to allow enable input batching
    INPUT_BATCH_MSGS = 1

    # if INPUT_BATCH_MSGS > 1, wait no longer than INPUT_BATCH_SECS after
    # first message, then process messages on hand:
    INPUT_BATCH_SECS = 120

    def __init__(self, process_name: str, descr: str):
        super().__init__(process_name, descr)
        self.input_msgs: List[InputMessage] = []
        self.input_timer: Optional[object] = None  # opaque timer
        # stopgap to make sure that the pipeline configurator has configurated the pipeline before we try to connect
        time.sleep(120)

    def main_loop(self) -> None:
        """
        basic main_loop for a consumer.
        override for a producer!
        """
        assert self.connection
        chan = self.connection.channel()
        chan.tx_select()  # enter transaction mode
        # set "prefetch" limit so messages get distributed among workers:
        chan.basic_qos(prefetch_count=self.INPUT_BATCH_MSGS * 2)

        arguments = {}
        # if batching multiple input messages,
        # set consumer timeout accordingly
        if self.INPUT_BATCH_MSGS > 1 and self.INPUT_BATCH_SECS:
            # add a small grace period, convert to milliseconds
            ms = (self.INPUT_BATCH_SECS + 10) * 1000
            arguments["x-consumer-timeout"] = ms
        chan.basic_consume(self.input_queue_name, self.on_message, arguments=arguments)

        chan.start_consuming()  # enter pika main loop; calls on_message

    def on_message(
        self,
        chan: BlockingChannel,
        method: Basic.Deliver,
        properties: BasicProperties,
        body: bytes,
    ) -> None:
        """
        basic_consume callback function
        """

        logger.debug("on_message %s", method.delivery_tag)  # no preformat!

        self.input_msgs.append(InputMessage(method, properties, body))

        if len(self.input_msgs) < self.INPUT_BATCH_MSGS:
            # here only when batching multiple msgs
            if self.input_timer is None and self.INPUT_BATCH_SECS and self.connection:
                # start timeout, in case less than a full batch is available
                self.input_timer = self.connection.call_later(
                    self.INPUT_BATCH_SECS, lambda: self._process_messages(chan)
                )
            return

        # here with full batch: start processing
        if self.input_timer and self.connection:
            self.connection.remove_timeout(self.input_timer)
            self.input_timer = None

        self._process_messages(chan)

    def _process_messages(self, chan: BlockingChannel) -> None:
        """
        Here w/ INPUT_BATCH_MSGS or
        INPUT_BATCH_SECS elapsed after first message
        """
        # NOTE! when INPUT_BATCH_MSGS != 1, timing for entire batch
        # (not normalized per message): actual work is done by
        # "end_of_batch" method

        with self.timer("process_msgs"):
            for m, p, b in self.input_msgs:
                # XXX wrap in try, handle retry, quarantine, increment counters
                self.process_message(chan, m, p, b)

            self.end_of_batch(chan)

            # ack message(s)
            multiple = len(self.input_msgs) > 1
            tag = self.input_msgs[-1].method.delivery_tag  # tag from last message
            assert tag is not None
            logger.debug("ack %s %s", tag, multiple)  # NOT preformated!!
            chan.basic_ack(delivery_tag=tag, multiple=multiple)
            self.input_msgs = []

            # AFTER basic_ack!
            chan.tx_commit()  # commit sent messages and ack atomically!

        sys.stdout.flush()  # for redirection, supervisord

    def process_message(
        self,
        chan: BlockingChannel,
        method: Basic.Deliver,
        properties: BasicProperties,
        body: bytes,
    ) -> None:
        raise AppException("Worker.process_message not overridden")

    def end_of_batch(self, chan: BlockingChannel) -> None:
        """hook for batch processors (ie; write to database)"""


class StoryWorker(Worker):
    """
    Process Stories in Queue Messages
    """

    def process_message(
        self,
        chan: BlockingChannel,
        method: Basic.Deliver,
        properties: BasicProperties,
        body: bytes,
    ) -> None:
        # XXX pass content-type?
        story = BaseStory.load(body)
        self.process_story(chan, story)

    def process_story(
        self,
        chan: BlockingChannel,
        story: BaseStory,
    ) -> None:
        raise NotImplementedError("Worker.process_story not overridden")

    def send_story(
        self,
        chan: BlockingChannel,
        story: BaseStory,
        exchange: Optional[str] = None,
        routing_key: str = DEFAULT_ROUTING_KEY,
    ) -> None:
        self.send_message(chan, story.dump(), exchange, routing_key)


class BatchStoryWorker(StoryWorker):
    """
    process batches of stories:
    INPUT_BATCH_MSGS controls batch size (and defaults to one),
    so you likely want to increase it, BUT, it's not prohibited,
    in case you want to test code on REALLY small batches!
    """

    def __init__(self, process_name: str, descr: str):
        super().__init__(process_name, descr)
        self._stories: List[BaseStory] = []
        if self.INPUT_BATCH_MSGS == 1:
            logger.info("INPUT_BATCH_MSGS is 1!!")

    def process_story(
        self,
        chan: BlockingChannel,
        story: BaseStory,
    ) -> None:
        self._stories.append(story)

    def end_of_batch(self, chan: BlockingChannel) -> None:
        self.story_batch(chan, self._stories)
        self._stories = []

    def story_batch(self, chan: BlockingChannel, stories: List[BaseStory]) -> None:
        raise NotImplementedError("BatchStoryWorker.story_batch not overridden")


def run(klass: type[Worker], *args: Any, **kw: Any) -> None:
    """
    run worker process, takes Worker subclass
    could, in theory create threads or asyncio tasks.
    """
    worker = klass(*args, **kw)
    worker.main()
