"""
Pipeline Worker Definitions.
Written to be a generic utility package.
Tries to hide Pika/RabbitMQ/AMQP as much as reasonably possible.

Story-specific things are in storyworker.py
"""

# NOTE!!!! This file has been CAREFULLY coded to NOT assume consumers
# are recieving messages from exactly one channel/queue:
# * There is no channel global/member!!!
# * The code DOES assume there is only one Pika connection.
# * For code processing messages: Pika ops MUST be done from Pika thread

# log.debug calls w/ "move to debug?" comments
# can be acted upon once the Pika-thread code is trusted.

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
from indexer.app import App, AppException, run

logger = logging.getLogger(__name__)
ptlogger = logging.getLogger("Pika-thread")  # used for logging from Pika-thread

DEFAULT_EXCHANGE = ""  # routes to queue named by routing key
DEFAULT_ROUTING_KEY = "default"

# default consumer timeout (for ack) is 30 minutes:
# https://www.rabbitmq.com/consumers.html#acknowledgement-timeout
CONSUMER_TIMEOUT_SECONDS = 30 * 60

# semaphore in the sense of railway signal tower!
# an exchange rather than a queue to avoid crocks to not monitor it!
_CONFIGURED_SEMAPHORE_EXCHANGE = "mc-configuration-semaphore"

# Media Cloud headers where code examines values:
RETRIES_HDR = "x-mc-retries"
EXCEPTION_HDR = "x-mc-what"

# MAX_RETRIES * RETRY_DELAY_MINUTES determines how long stories will be retried
# before quarantine:
MAX_RETRIES = 10
RETRY_DELAY_MINUTES = 60
MS_PER_MINUTE = 60 * 1000


class QuarantineException(AppException):
    """
    Exception for Worker code to raise when a message cannot
    _possibly_ be processed, and the message should be sent directly
    to jail (do not pass go, do not collect $200).

    Constructor argument should be a description, or repr(exception)
    """


class InputMessage(NamedTuple):
    """
    would prefer _channel, but not allowed for NamedTuple,
    maybe a DataObject would be better?
    """

    channel: BlockingChannel
    method: Basic.Deliver
    properties: BasicProperties
    body: bytes
    mtime: float  # time.monotonic() recv time


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


# Pika (AMQP) Library log message substrings to ignore when checking
# if queues available.  Pika is PAINFULLY verbose (lots of logging at
# INFO level) during normal operation!!  Trying NOT to filter out
# anything that might indicate the cause of an abnormal failure!!
# (otherwise
_PIKA_IGNORE_SUBSTRINGS = (
    "Connection refused",
    "ConnectionRefusedError",
    "Error in _create_connection",  # ERROR w/ exception
    "NOT_FOUND - no exchange",  # WARNING
    "Normal shutdown",
    "TimeoutError",
)


def _pika_message_filter(msg: logging.LogRecord) -> bool:
    """
    Filter applied to root handlers during _test_configured.
    return False to drop msg, True to keep.
    """
    # show non-pika messages
    if not msg.name.startswith("pika."):
        return True

    # ignore INFO and DEBUG messages
    # maybe ALWAYS suppress them with getLogger("pika").setLevel()?
    if msg.levelno <= logging.INFO:
        return False

    formatted = msg.getMessage()  # format message
    for substr in _PIKA_IGNORE_SUBSTRINGS:
        if substr in formatted:
            return False

    return True


class QApp(App):
    """
    Base class for AMQP/pika based App.
    Producers (processes that have no input queue)
    should derive from this class.

    BUT, this class knows nothing about Stories.
    BY DESIGN (see StoryMixin)
    """

    # set to False to delay connecting until self.qconnect called
    AUTO_CONNECT = True

    # pika logs (a lot) at INFO level: make logging.WARNING the default?
    # this default can be overridden with "--log-level pika:info"
    PIKA_LOG_DEFAULT: Optional[int] = None

    # override to False to avoid waiting until configuration done
    WAIT_FOR_QUEUE_CONFIGURATION = True

    # override to True for long-running message-sending QApps
    # (Pika thread causes problems for utilities that do blocking calls)
    START_PIKA_THREAD = False

    def __init__(self, process_name: str, descr: str):
        super().__init__(process_name, descr)

        self.connection: Optional[BlockingConnection] = None

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

    def _test_configured(self) -> bool:
        """
        NOTE! Called before Pika thread launched,
        uses own connection, and closes it
        """
        assert self.args and self.args.amqp_url
        url = self.args.amqp_url
        conn = None

        for handler in logging.root.handlers:
            handler.addFilter(_pika_message_filter)

        params = URLParameters(url)
        try:
            conn = BlockingConnection(params)
            chan = conn.channel()
            # throws ChannelClosedByBroker if exchange does not exist
            chan.exchange_declare(_CONFIGURED_SEMAPHORE_EXCHANGE, passive=True)
            return True
        except (
            requests.exceptions.ConnectionError,
            pika.exceptions.AMQPConnectionError,
            pika.exceptions.ChannelClosedByBroker,  # exchange not found
        ):
            return False
        finally:
            if conn and conn.is_open:
                conn.close()  # XXX wrap in try??
                # XXX need to process events?
            for handler in logging.root.handlers:
                handler.removeFilter(_pika_message_filter)

    def wait_until_configured(self) -> None:
        """for use by QApps that set WAIT_FOR_QUEUE_CONFIGURATION = False"""
        while not self._test_configured():
            logger.info("sleeping...")
            time.sleep(30)

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
        self.connection = BlockingConnection(URLParameters(url))
        logger.info(f"connected to {url}")

        # start Pika I/O thread (ONLY ONE!)
        if self.START_PIKA_THREAD:
            self.start_pika_thread()

    def start_pika_thread(self) -> None:
        """
        Pika I/O thread. ONLY START ONE!
        Handles async messages from AMQP (ie; RabbitMQ) server,
        including connection keep-alive.
        """
        assert self.connection

        # need next check & _pika_thread set under a lock to allow
        # calling from any thread:
        assert threading.current_thread() == threading.main_thread()

        if self._pika_thread:
            logger.error("start_pika_thread called again")
            return

        self._pika_thread = threading.Thread(
            target=self._pika_thread_body, name="Pika-thread", daemon=True
        )
        self._pika_thread.start()

    def _subscribe(self) -> None:
        """
        Called from Pika thread with newly opened connection.
        overridden in Worker class to subscribe to input queues.

        NOTE! May open multiple channels, to different queues, with
        different pre-fetch limits to allow preferential treatment of
        messages from different sources (ie; new vs retries)
        """

    def _pika_thread_body(self) -> None:
        """
        Body for Pika-thread.  Processes all Pika I/O events.

        ALL channel methods MUST be executed via
        self._call_in_pika_thread to run here.
        """
        ptlogger.info("Pika thread starting")

        # hook for Workers to make consume calls,
        # (and/or any blocking calls, like exchange/queue creation)
        self._subscribe()

        try:
            # Timeout value means _running can be set to False and main thread
            # may have to wait for timeout before this thread wakes up and exits.
            while self._running and self.connection and self.connection.is_open:
                # process_data_events is called by conn.sleep,
                # but may return sooner:
                self.connection.process_data_events(10)
        finally:
            # Trying clean close, in case process_data_events returns
            # with unprocessed events (especially send callbacks).
            if self.connection and self.connection.is_open:
                self.connection.close()
            self.connection = None
            self._running = False  # tell _process_messages

            # here if _running was set False, connection closed, exception thrown
            ptlogger.info("Pika thread exiting")

    def _call_in_pika_thread(self, cb: Callable[[], None]) -> None:
        assert self.connection

        # XXX this will need a lock if app runs in multiple threads
        if self._pika_thread is None:
            # here from a QApp
            # transactions will NOT be enabled
            # (unless _subscribe is overridden)
            self.start_pika_thread()

        self.connection.add_callback_threadsafe(cb)

    def _stop_pika_thread(self) -> None:
        if self._pika_thread:
            if self._pika_thread.is_alive():
                self._running = False
                # Log message in case Pika thread hangs.
                logger.info("Waiting for Pika thread to exit")
                # could issue join with timeout.
                self._pika_thread.join()
            self._pika_thread = None

    def cleanup(self) -> None:
        super().cleanup()
        # saw error "Fatal Python error: _enter_buffered_busy: could
        #   not acquire lock for <_io.BufferedWriter name='<stderr>'> at
        #   interpreter shutdown, possibly due to daemon threads"
        # so asking Pika thread to exit, and waiting for it.
        self._stop_pika_thread()

    def _send_message(
        self,
        chan: BlockingChannel,
        data: bytes,
        exchange: Optional[str] = None,
        routing_key: str = DEFAULT_ROUTING_KEY,
        properties: Optional[BasicProperties] = None,  # WILL BE MODIFIED!
    ) -> None:
        """
        called by Worker/Publisher code in main thread.
        It would be cleaner to pass InputMessage object with send methods to Workers,
        so bare channel is never exposed to worker code.  Maybe later.
        """
        if exchange is None:
            exchange = self.output_exchange_name

        if properties is None:
            properties = BasicProperties()

        # persist messages on disk
        # (otherwise may be lost on reboot)
        # also pika.DeliveryMode.Persistent.value, but not in typing stubs?
        properties.delivery_mode = PERSISTENT_DELIVERY_MODE

        def sender() -> None:
            ptlogger.debug(
                "send exch '%s' key '%s' %d bytes", exchange, routing_key, len(data)
            )
            chan.basic_publish(exchange, routing_key, data, properties)

        self._call_in_pika_thread(sender)

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
        port = par.port + 10000  # default 15672
        api = rabbitmq_admin.AdminAPI(
            url=f"http://{par.host}:{port}", auth=(creds.username, creds.password)
        )
        return api


class Worker(QApp):
    """
    Base class for Workers that consume messages
    (knows nothing about stories)
    """

    START_PIKA_THREAD = True

    def __init__(self, process_name: str, descr: str):
        super().__init__(process_name, descr)
        self._message_queue: queue.Queue[InputMessage] = queue.Queue()

     def define_options(self, ap: argparse.ArgumentParser) -> None:
        ap.add_argument(
            "--from-quarantine",
            action="store_true",
            default=False,
            help="Take input from quarantine queue",
        )

    def process_args(self) -> None:
        assert self.args
        if self.args.from_quarantine:
            self.input_queue_name = quarantine_queue_name(self.process_name)

    def main_loop(self) -> None:
        """
        basic main_loop for a consumer.
        override for a producer!
        """

        self._process_messages()

    def _on_message(
        self,
        chan: BlockingChannel,
        method: Basic.Deliver,
        properties: BasicProperties,
        body: bytes,
    ) -> None:
        """
        basic_consume callback function; called in Pika thread.
        Queue InputMessage for Worker thread _process_messages function,
        ack will be done back in Pika thread.
        """
        im = InputMessage(chan, method, properties, body, time.monotonic())
        ptlogger.info("on_message tag #%s", method.delivery_tag)  # move to debug?
        self._message_queue.put(im)

    def _subscribe(self) -> None:
        """
        Called from Pika thread with newly opened connection.
        """
        assert self.connection
        chan = self.connection.channel()

        # enter transaction mode for atomic transmit & ack.
        # tx_commit must be called after any sends or acks!!!
        # (first send or ACK implicitly opens a transaction)
        chan.tx_select()

        self._qos(chan)

        # subscribe to the queue.
        chan.basic_consume(self.input_queue_name, self._on_message)

    def _qos(self, chan: BlockingChannel) -> None:
        """
        set "prefetch" limit: distributes messages among workers
        processes, limits the number of unacked messages queued
        """
        # double buffered: one to work on, one on deck
        chan.basic_qos(prefetch_count=2)

    def _process_messages(self) -> None:
        """
        Blocking loop for running Worker processing code.  Processes
        messages queued by _on_message (called from Pika thread).
        May run in multiple threads!
        """

        while self._running:
            im = self._message_queue.get()  # blocking
            self._process_one_message(im)
            self._ack_and_commit(im)

            sys.stdout.flush()  # for redirection, supervisord
        logger.info("_process_messages exiting")
        sys.exit(1)  # give error status so docker restarts

    def _process_one_message(self, im: InputMessage) -> bool:
        """
        Call process_message method, handling retries and quarantine
        """
        tag = im.method.delivery_tag
        assert tag is not None
        logger.info("_process_one_message #%s", tag)  # move to debug?
        t0 = time.monotonic()
        # XXX report t0-im.mtime as latency since message queued timing stat?

        try:
            self.process_message(im)
            status = "ok"
        except QuarantineException as e:
            status = "error"
            self._quarantine(im, e)
        except Exception as e:
            if self._retry(im, e):
                status = "retry"
            else:
                status = "retryx"  # retries eXausted

        ms = 1000 * (time.monotonic() - t0)
        # NOTE! statsd timers have .count but not .rate
        self.timing("message", ms, [("stat", status)])
        logger.info(
            "processed #%s in %.3f ms, status: %s", tag, ms, status
        )  # move to debug?

        return status == "ok"

    def _ack_and_commit(self, im: InputMessage, multiple: bool = False) -> None:
        """
        a closure wrapped in a method

        ("A riddle wrapped in a mystery inside an enigma" -- Churchill)

        The closure is declared in a method rather than inline in the
        _process message loop because a closure in a loop captures the
        (method scope) loop variable whose value may change before the
        closure is called!

        This avoids using functools.partial, which I find less
        illustrative of a function call with captured values. -phil
        """
        tag = im.method.delivery_tag  # tag from last message
        assert tag is not None

        def acker() -> None:
            ptlogger.info("ack and commit #%s", tag)  # move to debug?

            im.channel.basic_ack(delivery_tag=tag, multiple=multiple)

            # AFTER basic_ack!
            im.channel.tx_commit()  # commit sent messages and ack atomically!

        self._call_in_pika_thread(acker)

    def _exc_headers(self, e: Exception) -> Dict:
        """
        return dict of headers to add to a message
        after an exception was caught
        """

        # str(exception) omits class name.
        # truncate because Unicode exceptions contain ENTIRE body
        # which creates impossibly long headers!
        what = repr(e)[:100]

        ret = {
            "x-mc-who": self.process_name,
            "x-mc-when": str(time.time()),
            EXCEPTION_HDR: what,
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

    def _quarantine(self, im: InputMessage, e: Exception) -> None:
        """
        Here from QuarantineException OR on other exception
        and retries exhausted
        """

        headers = self._exc_headers(e)
        logger.info(f"quarantine: {headers[EXCEPTION_HDR]}")  # TEMP

        # send to quarantine via direct exchange w/ headers
        self._send_message(
            im.channel,
            im.body,
            DEFAULT_EXCHANGE,
            quarantine_queue_name(self.process_name),
            BasicProperties(headers=headers),
        )

    def _retry(self, im: InputMessage, e: Exception) -> bool:
        """
        returns False if retries exhausted
        """
        oh = im.properties.headers  # old headers
        if oh:
            retries = oh.get(RETRIES_HDR, 0)
            if retries >= MAX_RETRIES:
                self._quarantine(im, e)
                return False  # retries exhausted
        else:
            retries = 0

        headers = self._exc_headers(e)
        headers[RETRIES_HDR] = retries + 1

        logger.info(f"retry #{retries} failed: {headers[EXCEPTION_HDR]}")

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
        self._send_message(
            im.channel,
            im.body,
            DEFAULT_EXCHANGE,
            self.delay_queue_name,
            props,
        )
        return True  # queued for retry

    def process_message(self, im: InputMessage) -> None:
        raise NotImplementedError("Worker.process_message not overridden")


# story-related classes etc moved to storyworker.py
