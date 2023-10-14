"""
Pipeline Worker Definitions
"""

import argparse
import logging
import os
import pickle
import queue
import sys
import threading
import time
from typing import Any, Callable, Dict, List, NamedTuple, Optional

import pika.credentials
import pika.exceptions
import rabbitmq_admin
import requests.exceptions

# PyPI
from pika import BasicProperties
from pika.adapters.blocking_connection import BlockingChannel, BlockingConnection
from pika.connection import URLParameters
from pika.spec import PERSISTENT_DELIVERY_MODE, Basic

# story-indexer
from indexer.app import App, AppException
from indexer.story import BaseStory

logger = logging.getLogger(__name__)

# content types:
MIME_TYPE_PICKLE = "application/python-pickle"

DEFAULT_ROUTING_KEY = "default"

# semaphore in the sense of railway signal tower!
# an exchange rather than a queue to avoid crocks to not monitor it!
_CONFIGURED_SEMAPHORE_EXCHANGE = "mc-configuration-semaphore"

# default consumer timeout (for ack) is 30 minutes:
# https://www.rabbitmq.com/consumers.html#acknowledgement-timeout
CONSUMER_TIMEOUT = 30 * 60

RETRIES_HDR = "x-mc-retries"

# MAX_RETRIES * RETRY_DELAY_MINUTES determines how long stories will be retried
# before quarantine:
MAX_RETRIES = 10
RETRY_DELAY_MINUTES = 60

MS_PER_MINUTE = 60 * 1000


class QuarantineException(AppException):
    """
    Exception to raise when a message cannot _possibly_ be processed,
    and the message should be sent directly to jail
    (do not pass go, do not collect $200).

    Constructor argument should be a description, or repr(exception)
    """


class InputMessage(NamedTuple):
    """used to save batches of input messages"""

    channel: BlockingChannel
    method: Basic.Deliver
    properties: BasicProperties
    body: bytes


# NOTE!! base_queue_name depends on the following
# functions adding ONLY a hyphen and a single word!


def input_queue_name(procname: str) -> str:
    """take process name, return input queue name"""
    # Every consumer has an an input queue NAME-in.
    return procname + "-in"


def output_exchange_name(procname: str) -> str:
    """take process name, return input exchange name"""
    # Every producer has an output exchange NAME-out
    # with links to downstream input queues.
    return procname + "-out"


def quarantine_queue_name(procname: str) -> str:
    """take process name, return quarantine queue name"""
    # could have a single quarantine queue
    # (and requeue based on 'x-mc-from' if needed),
    # but having a quarantine queue per worker queue
    # makes it clear where the problem is, and
    # avoids having to chew through a mess of messages.
    return procname + "-quar"


def delay_queue_name(procname: str) -> str:
    """take process name, return retry delay queue name"""
    return procname + "-delay"


def base_queue_name(qname: str) -> str:
    """
    take a queue name, and return base (app) name
    """
    return qname.rsplit("-", maxsplit=1)[0]


class QApp(App):
    """
    Base class for AMQP/pika based App.
    Producers (processes that have no input queue)
    should derive from this class
    """

    # set to False to delay connecting until self.qconnect called
    AUTO_CONNECT = True

    # pika logs (a lot) at INFO level: make logging.WARNING the default?
    # this default can be overridden with "--log-level pika:info"
    PIKA_LOG_DEFAULT: Optional[int] = None

    # override to False to avoid waiting until configuration done
    WAIT_FOR_QUEUE_CONFIGURATION = True

    def __init__(self, process_name: str, descr: str):
        super().__init__(process_name, descr)

        self._conn: Optional[BlockingConnection] = None

        self._pika_thread: Optional[threading.Thread] = None
        self._running = True

        # queues/exchanges created using indexer.pipeline:
        self.input_queue_name = input_queue_name(self.process_name)
        self.output_exchange_name = output_exchange_name(self.process_name)
        self.delay_queue_name = delay_queue_name(self.process_name)

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
        ap.add_argument(
            "--from-quarantine",
            action="store_true",
            default=False,
            help="Take input from quarantine queue",
        )

        if self.PIKA_LOG_DEFAULT is not None:
            logging.getLogger("pika").setLevel(self.PIKA_LOG_DEFAULT)

    def process_args(self) -> None:
        super().process_args()

        assert self.args
        if not self.args.amqp_url:
            logger.fatal("need --rabbitmq-url or RABBITMQ_URL")
            sys.exit(1)

        if self.args.from_quarantine:
            self.input_queue_name = quarantine_queue_name(self.process_name)

        if self.AUTO_CONNECT:
            self.qconnect()

    def _test_configured(self) -> bool:
        """
        NOTE! Called before Pika thread launched,
        uses own connection, and closes it
        """
        assert self.args and self.args.amqp_url
        url = self.args.amqp_url
        try:
            conn = BlockingConnection(URLParameters(url))
        except (
            requests.exceptions.ConnectionError,
            pika.exceptions.AMQPConnectionError,
        ):
            return False

        # XXX do getLogger("pika").addFilter(filter) to avoid noise???
        try:
            chan = conn.channel()
            # throws ChannelClosedByBroker if exchange does not exist
            chan.exchange_declare(_CONFIGURED_SEMAPHORE_EXCHANGE, passive=True)
            return True
        except pika.exceptions.ChannelClosedByBroker:  # exchange not found
            return False
        finally:
            # XXX remove pika message filter
            if conn and conn.is_open:
                conn.close()  # XXX wrap in try??
                # XXX need to process events?

    def wait_until_configured(self) -> None:
        """for use by QApps that set WAIT_FOR_QUEUE_CONFIGURATION = False"""
        while not self._test_configured():
            time.sleep(5)

    def _set_configured(self, chan: BlockingChannel, set_true: bool) -> None:
        """INTERNAL: for use by indexer.pipeline ONLY!"""
        if set_true:
            chan.exchange_declare(_CONFIGURED_SEMAPHORE_EXCHANGE)
        else:
            chan.exchange_delete(_CONFIGURED_SEMAPHORE_EXCHANGE)

    def qconnect(self) -> None:
        """
        called from process_args if AUTO_CONNECT is True
        """
        if self.WAIT_FOR_QUEUE_CONFIGURATION:
            logger.info("waiting until queues configured....")
            self.wait_until_configured()
            logger.info("queues configured")

        assert self.args  # checked in process_args
        url = self.args.amqp_url
        assert url  # checked in process_args
        self._conn = BlockingConnection(URLParameters(url))
        assert self._conn  # keep mypy quiet

        logger.info(f"connected to {url}")

    def _run_pika_thread(self) -> None:
        """
        called after channels are open; launches pika I/O thread
        """
        self._pika_thread = threading.Thread(
            target=self._pika_thread_body, name="Pika-thread"
        )
        self._pika_thread.daemon = True  # remove need to join before exit???
        self._pika_thread.run()

        # XXX wait on queue here?

    def _pika_thread_body(self) -> None:
        """
        Body for Pika-thread.
        Processes all Pika I/O events.
        Any channel methods MUST be executed via
        connection.add_callback_threadsafe()
        """
        logging.info("Pika thread starting")
        while self._running and self._conn and self._conn.is_open:
            # process_data_events is called by conn.sleep,
            # but may return sooner:
            self._conn.process_data_events(10)
        logging.info("Pika thread exiting")

    def _call_in_pika_thread(self, cb: Callable[[], None]) -> None:
        """
        It would be cleaner to pass InputMessage object with send methods to Workers,
        so bare channel is never exposed to worker code.  Maybe later.
        """
        assert self._conn
        self._conn.add_callback_threadsafe(cb)

    def send_message(
        self,
        chan: BlockingChannel,
        data: bytes,
        exchange: Optional[str] = None,
        routing_key: str = DEFAULT_ROUTING_KEY,
        properties: Optional[BasicProperties] = None,
    ) -> None:
        """
        called by Worker/Publisher code in main thread
        """
        if exchange is None:
            exchange = self.output_exchange_name

        if properties is None:
            properties = BasicProperties()

        # persist messages on disk
        # (otherwise may be lost on reboot)
        # also pika.DeliveryMode.Persistent.value, but not in typing stubs?
        properties.delivery_mode = PERSISTENT_DELIVERY_MODE

        def send() -> None:
            chan.basic_publish(exchange, routing_key, data, properties)

        self._call_in_pika_thread(send)

        if exchange:
            dest = exchange
        else:
            dest = routing_key  # using default exchange
        self.incr("sent-msgs", labels=[("dest", dest)])

    def admin_api(self) -> rabbitmq_admin.AdminAPI:  # type: ignore[no-any-unimported]
        args = self.args
        assert args

        par = URLParameters(args.amqp_url)
        creds = par.credentials
        assert isinstance(creds, pika.credentials.PlainCredentials)
        port = 15672  # XXX par.port + 10000???
        api = rabbitmq_admin.AdminAPI(
            url=f"http://{par.host}:{port}", auth=(creds.username, creds.password)
        )
        return api


class Worker(QApp):
    """Base class for Workers that consume messages"""

    def __init__(self, process_name: str, descr: str):
        super().__init__(process_name, descr)
        self._message_queue: queue.Queue[InputMessage] = queue.Queue()

    def main_loop(self) -> None:
        """
        basic main_loop for a consumer.
        override for a producer!
        """

        assert self._conn
        chan = self._conn.channel()
        chan.tx_select()  # enter transaction mode
        # set "prefetch" limit: distributes messages among workers,'
        # limits the number of unacked messages in _message_queue
        chan.basic_qos(prefetch_count=2)
        chan.basic_consume(self.input_queue_name, self.on_message)

        self._run_pika_thread()
        self._process_messages()

    def on_message(
        self,
        chan: BlockingChannel,
        method: Basic.Deliver,
        properties: BasicProperties,
        body: bytes,
    ) -> None:
        """
        basic_consume callback function; called in Pika thread.
        Queue message for Worker thread _process_messages function.
        """
        logger.debug("on_message %s", method.delivery_tag)  # no preformat!
        self._message_queue.put(InputMessage(chan, method, properties, body))

    def _process_messages(self) -> None:
        """
        Blocking loop for Worker thread
        """

        while True:
            im: InputMessage = self._message_queue.get()  # blocking
            t0 = time.monotonic()
            # XXX report latency since message queued??
            try:
                # XXX pass im, with methods and non-public channel!
                self.process_message(im.channel, im.method, im.properties, im.body)
                status = "ok"
            except QuarantineException as e:
                status = "error"
                self._quarantine(im.channel, im.method, im.properties, im.body, e)
            except Exception as e:
                status = "retry"
                self._retry(im.channel, im.method, im.properties, im.body, e)

            tag = im.method.delivery_tag  # tag from last message
            assert tag is not None
            logger.debug("ack %s", tag)  # NOT preformated!!

            def ack_and_commit() -> None:
                im.channel.basic_ack(delivery_tag=tag)
                # AFTER basic_ack!
                im.channel.tx_commit()  # commit sent messages and ack atomically!

            self._call_in_pika_thread(ack_and_commit)
            sys.stdout.flush()  # for redirection, supervisord

            ms_per_msg = 1000 * (time.monotonic() - t0)
            # NOTE! also serves as message counter!
            self.timing("message", ms_per_msg, [("stat", status)])

    def _exc_headers(self, e: Exception) -> Dict:
        """
        return dict of headers to add to a message
        after an exception was caught
        """

        ret = {
            "x-mc-who": self.process_name,
            "x-mc-what": repr(e)[:50],  # str() omits exception class name
            "x-mc-when": str(time.time()),
            # maybe log hostname @ time w/ full traceback
            # and include hostname in headers (to find full traceback)
        }

        # advance to innermost traceback
        tb = e.__traceback__
        while tb:
            next = tb.tb_next
            if not next:
                break
            tb = next

        if tb:
            code = tb.tb_frame.f_code
            fname = code.co_filename
            lineno = tb.tb_lineno
            func = code.co_name
            ret["x-mc-where"] = f"{fname}:{lineno}"
            ret["x-mc-name"] = func  # typ. function name

        return ret

    def _quarantine(
        self,
        chan: BlockingChannel,
        method: Basic.Deliver,
        properties: BasicProperties,
        body: bytes,
        e: Exception,
    ) -> None:
        """
        Here from QuarantineException OR on other exception
        and retries exhausted
        """
        logger.info(f"quarantine: {e}")  # TEMP

        headers = self._exc_headers(e)

        # send to quarantine via direct exchange w/ headers
        self.send_message(
            chan,
            body,
            "",
            quarantine_queue_name(self.process_name),
            BasicProperties(headers=headers),
        )

    def _retry(
        self,
        chan: BlockingChannel,
        method: Basic.Deliver,
        properties: BasicProperties,
        body: bytes,
        e: Exception,
    ) -> None:
        logger.info(f"retry: {e!r}")  # TEMP

        # XXX if debugging re-raise exception???

        oh = properties.headers  # old headers
        if oh:
            retries = oh.get(RETRIES_HDR, 0)
            if retries >= MAX_RETRIES:
                self._quarantine(chan, method, properties, body, e)
                return
        else:
            retries = 0

        headers = self._exc_headers(e)
        headers[RETRIES_HDR] = retries + 1

        # Queue message to -delay queue, which has no consumers with
        # an expiration/TTL; when messages expire, they are routed
        # back to the -in queue via dead-letter-{exchange,routing-key}.

        # Would like exponential backoff (BASE << retries),
        # but https://www.rabbitmq.com/ttl.html says:
        #    When setting per-message TTL expired messages can queue
        #    up behind non-expired ones until the latter are consumed
        #    or expired.
        expiration_ms_str = str(int(RETRY_DELAY_MINUTES * MS_PER_MINUTE))

        # send to retry delay queue via default exchange
        props = BasicProperties(headers=headers, expiration=expiration_ms_str)
        self.send_message(chan, body, "", self.delay_queue_name, props)

    def process_message(
        self,
        chan: BlockingChannel,
        method: Basic.Deliver,
        properties: BasicProperties,
        body: bytes,
    ) -> None:
        raise NotImplementedError("Worker.process_message not overridden")

    def send_story(
        self,
        chan: BlockingChannel,
        story: BaseStory,
        exchange: Optional[str] = None,
        routing_key: str = DEFAULT_ROUTING_KEY,
    ) -> None:
        self.send_message(chan, story.dump(), exchange, routing_key)


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
        raise NotImplementedError("StoryWorker.process_story not overridden")


def run(klass: type[Worker], *args: Any, **kw: Any) -> None:
    """
    run worker process, takes Worker subclass
    could, in theory create threads or asyncio tasks.
    """
    worker = klass(*args, **kw)
    worker.main()
