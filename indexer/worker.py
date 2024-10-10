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

import argparse
import logging
import os
import queue
import sys
import threading
import time
from enum import Enum
from typing import Callable, Dict, NamedTuple, Optional, Tuple, Type

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

logger = logging.getLogger(__name__)

# for use w/ -L option:
msglogger = logging.getLogger(__name__ + ".msgs")

DEFAULT_EXCHANGE = ""  # routes to queue named by routing key
DEFAULT_ROUTING_KEY = "default"

# default consumer timeout (for ack) is 30 minutes:
# https://www.rabbitmq.com/consumers.html#acknowledgement-timeout
CONSUMER_TIMEOUT_SECONDS = 30 * 60

# Semaphore in the sense of railway signal tower!
# An exchange rather than a queue to avoid crocks to not monitor it!
# deploy.sh puts a unique ID in DEPLOYMENT_ID in docker-compose.yml,
# so workers will wait until the matching version of pipeline.py has
# configured things.

DEPLOYMENT_ID = "DEPLOYMENT_ID"
_CONFIGURED_SEMAPHORE_EXCHANGE = os.environ.get(DEPLOYMENT_ID)

# Media Cloud headers where code examines values:
RETRIES_HDR = "x-mc-retries"
EXCEPTION_HDR = "x-mc-what"

MS_PER_MINUTE = 60 * 1000
SECONDS_PER_DAY = 24 * 60 * 60


class QuarantineException(AppException):
    """
    Exception for Worker code to raise when a message cannot
    _possibly_ be processed, and the message should be sent directly
    to jail (do not pass go, do not collect $200).

    Constructor argument should be a description, or repr(exception)
    """


class RequeueException(AppException):
    """
    Exception for Worker code to requeue message (preserving headers)
    for QUICK reprocessing.

    Requires ...-fast queue to exist
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


def fast_queue_name(procname: str) -> str:
    """take process name, return fast delay queue name"""
    return procname + "-fast"


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


class PikaThreadState(Enum):
    NOT_STARTED = 0  # initial state
    STARTED = 1  # set by start_pika_thread (by Main thread)
    RUNNING = 2  # set by _pika_thread_body (by Pika thread)
    STOPPING = 3  # set via _stop_pika_thread (by stop in Pika thread)
    STOPPED = 4  # set at end of _pika_thread_body (by Pika thread)


class QApp(App):
    """
    Base class for AMQP/pika based App.
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
        self._state = PikaThreadState.NOT_STARTED
        self._app_errors = False

        # debugging aid to use with --from-quarantine:
        self._crash_on_exception = os.environ.get("WORKER_EXCEPTION_CRASH", "") != ""

        self.sent_messages = 0

        # queues/exchanges created using indexer.pipeline:
        self.input_queue_name = input_queue_name(self.process_name)
        self.output_exchange_name = output_exchange_name(self.process_name)
        self.delay_queue_name = delay_queue_name(self.process_name)
        self.fast_queue_name = fast_queue_name(self.process_name)

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
            help=f"override RABBITMQ_URL (default {default_url})",
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

        if not _CONFIGURED_SEMAPHORE_EXCHANGE:
            # allow user testing outside docker
            logger.warning("%s not set", DEPLOYMENT_ID)
            return True

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
        if not _CONFIGURED_SEMAPHORE_EXCHANGE:
            # error to run pipeline.py configure command without
            # a deployment id set in environment.
            raise Exception(f"{DEPLOYMENT_ID} not set")

        # remove old semaphores.  (deploy on a running stack doesn't
        # restart RabbitMQ container?  and RabbitMQ outside a
        # container will keep old semaphores indefinitely)
        api = self.admin_api()
        for exchange in api.list_exchanges():
            name = exchange["name"]
            if (
                name.startswith("mc-configuration-")
                and name != _CONFIGURED_SEMAPHORE_EXCHANGE
            ):
                chan.exchange_delete(name)

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

    def _assert_main_thread(self) -> None:
        assert threading.current_thread() == threading.main_thread()

    def start_pika_thread(self) -> None:
        """
        Start Pika I/O thread. ONLY START ONE!
        """
        self._assert_main_thread()

        assert self.connection
        if self._state != PikaThreadState.NOT_STARTED:
            logger.error("start_pika_thread state %s", self._state)
            return

        self._state = PikaThreadState.STARTED
        self._pika_thread = threading.Thread(
            target=self._pika_thread_body, name="Pika", daemon=True
        )
        self._pika_thread.start()

        for x in range(0, 5):
            if self._state == PikaThreadState.RUNNING:
                return
            time.sleep(x)
        logger.fatal("Pika thread did not start")
        sys.exit(1)

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
        Body for Pika-thread.  Processes all Pika I/O events:
        async messages from AMQP (ie; RabbitMQ) server,
        including connection keep-alive.

        Pika is not thread aware, so once started, the connection is
        owned by this thread, and ALL channel methods MUST be executed
        via self._call_in_pika_thread to run here.
        """
        logger.info("Pika thread starting")

        self._state = PikaThreadState.RUNNING

        try:
            # hook for Workers to make consume calls,
            # (and/or any blocking calls, like exchange/queue creation)
            self._subscribe()

            while True:
                if self._state != PikaThreadState.RUNNING:
                    logger.info("pika thread: state %s", self._state)
                    break
                if not (self.connection and self.connection.is_open):
                    logger.info("pika thread: connection closed")
                    break
                # Pika 1.3.2 sources accept None as an argument to block, but
                # types-pika 1.2.0b3 doesn't reflect that, so sleep 24 hrs.
                # _stop_pika_thread does _call_in_pika_thread(stop) to wake:
                self.connection.process_data_events(SECONDS_PER_DAY)

        finally:
            # tell _process_messages
            self._state = PikaThreadState.STOPPED
            self._pika_thread_cleanup()

            # Trying clean close, in case process_data_events returns
            # with unprocessed events (especially send callbacks).
            if self.connection and self.connection.is_open:
                logger.info("closing Pika connection")
                self.connection.close()
            self.connection = None

            logger.info("Pika thread exiting")

    def _call_in_pika_thread(self, cb: Callable[[], None]) -> None:

        if self._state == PikaThreadState.NOT_STARTED:
            # here from a QApp in Main thread
            # transactions will NOT be enabled
            # (unless _subscribe is overridden)
            self.start_pika_thread()  # returns with _state == RUNNING

        # RACES POSSIBLE FROM HERE ON, BUT ONLY ON SHUTDOWN/ERROR:
        if self._state != PikaThreadState.RUNNING:
            logger.info("Pika thread state %s: %s", self._state, cb.__name__)
            sys.exit(1)

        assert self._pika_thread
        assert self._pika_thread.is_alive()
        assert self.connection
        assert self.connection.is_open

        # NOTE! add_callback_threadsafe is documented (in the Pika
        # 1.3.2 comments) as the ONLY thread-safe connection method!!!
        self.connection.add_callback_threadsafe(cb)

    def _stop_pika_thread(self) -> None:
        """
        called from cleanup (below) after main_loop exit
        """
        self._assert_main_thread()
        if self._pika_thread:
            if self._state == PikaThreadState.RUNNING:
                logger.info("Stopping pika thread")

                # wake up Pika thread, and have it change _state
                # (after all other _call_in_pika_thread generated
                # requests procesed) to avoid loss of newly queued Stories.
                def stop() -> None:
                    logger.info("stop")
                    self._state = PikaThreadState.STOPPING

                self._call_in_pika_thread(stop)

                # Log message in case Pika thread hangs.
                logger.info("Waiting for Pika thread to exit")

                # could issue join with timeout.
                self._pika_thread.join()

    def _pika_thread_cleanup(self) -> None:
        """
        Override as needed
        """
        pass

    def cleanup(self) -> None:
        """
        called when main_loop returns
        """
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
            msglogger.debug(
                "send exch '%s' key '%s' %d bytes", exchange, routing_key, len(data)
            )
            chan.basic_publish(exchange, routing_key, data, properties)
            self.sent_messages += 1

        self._call_in_pika_thread(sender)

        if exchange:
            dest = exchange
        else:
            dest = routing_key  # using default exchange
        self.incr("sent-msgs", labels=[("dest", dest)])

    def admin_api(self) -> rabbitmq_admin.AdminAPI:
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

    def synchronize_with_pika_thread(self) -> bool:
        """
        block current thread until all messages queued
        (at the time the call is made) have been processed
        by Pika thread.  Created for use by queuers.
        Queue workers atomically output and ack.
        """
        # method local variable (can call independently in multiple threads!):
        done = False

        def sync() -> None:
            nonlocal done  # access method local
            done = True
            logger.info("sync")

        self._call_in_pika_thread(sync)

        loops = 0
        while self._state == PikaThreadState.RUNNING:
            if done:
                return True
            loops += 1
            if loops == 10:
                logger.warning("waiting for pika thread sync")
                loops = 0
            time.sleep(0.1)
        return False


class Worker(QApp):
    """
    Base class for Workers that consume messages
    (knows nothing about stories)
    """

    # MAX_RETRIES * RETRY_DELAY_MINUTES determines how long stories will be retried
    # before quarantine:
    MAX_RETRIES = 10
    RETRY_DELAY_MINUTES = 60

    # exception classes to discard instead of quarantine
    NO_QUARANTINE: Tuple[Type[Exception], ...] = ()

    # always start Pika thread:
    START_PIKA_THREAD = True

    def __init__(self, process_name: str, descr: str):
        super().__init__(process_name, descr)
        self._message_queue: queue.Queue[Optional[InputMessage]] = queue.Queue()

    def define_options(self, ap: argparse.ArgumentParser) -> None:
        super().define_options(ap)
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
        super().process_args()  # after setting self.input_queue_name

    def prefetch(self) -> int:
        # double buffer: one to work on, one on deck
        return 2

    def main_loop(self) -> None:
        """
        basic main_loop for a consumer.
        override for a producer!
        """
        self._process_messages()
        sys.exit(1)

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
        msglogger.debug("on_message tag #%s", method.delivery_tag)
        self._on_input_message(im)

    def _on_input_message(self, im: InputMessage) -> None:
        """
        called in Pika thread.
        override to interrupt direct delivery of messages to _message_queue
        """
        self._message_queue.put(im)

    _put_message_queue = _on_input_message

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

        # set "prefetch" limit: distributes messages among worker
        # processes, limits the number of unacked messages queued
        # to worker processes.
        prefetch = self.prefetch()
        assert prefetch > 0
        logger.info("prefetch %d", prefetch)
        chan.basic_qos(prefetch_count=prefetch)

        # subscribe to the queue.
        chan.basic_consume(self.input_queue_name, self._on_message)

    def _process_messages(self) -> None:
        """
        Blocking loop for running Worker processing code.  Processes
        messages queued by _on_message (called from Pika thread).
        May run in multiple threads!
        """

        while True:
            if self._state != PikaThreadState.RUNNING:
                logger.info("_process_messages state %s", self._state)
                break
            if self._app_errors:
                logger.info("_process_messages _app_errors")
                break
            im = self._message_queue.get()  # blocking
            if im is None:  # kiss of death?
                logger.info("_process_messages kiss of death")
                break
            self._process_one_message(im)
            self._ack_and_commit(im)
        logger.info("_process_messages returning")

    def _process_one_message(self, im: InputMessage) -> bool:
        """
        Call process_message method, handling retries and quarantine
        """
        tag = im.method.delivery_tag
        assert tag is not None
        msglogger.debug("_process_one_message #%s", tag)
        t0 = time.monotonic()
        # XXX report t0-im.mtime as latency since message queued timing stat?

        try:
            self.process_message(im)
            status = "ok"
        except QuarantineException as e:
            status = "error"
            self._quarantine(im, e)
        except RequeueException:
            status = "requeue"
            self._requeue(im)
        except Exception as e:
            if self._crash_on_exception:
                raise  # for debug
            if self._retry(im, e):
                status = "retry"
            else:
                status = "retryx"  # retries eXausted

        ms = 1000 * (time.monotonic() - t0)
        # NOTE! statsd timers have .count but not .rate
        self.timing("message", ms, [("stat", status)])
        msglogger.debug("processed #%s in %.3f ms, status: %s", tag, ms, status)

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

        def acker() -> None:
            self._pika_ack_and_commit(im, multiple)

        self._call_in_pika_thread(acker)

    def _pika_ack_and_commit(self, im: InputMessage, multiple: bool = False) -> None:
        """
        call ONLY from pika thread!!
        """
        tag = im.method.delivery_tag  # tag from last message
        assert tag is not None

        chan = im.channel

        msglogger.debug("ack and commit #%s", tag)
        chan.basic_ack(delivery_tag=tag, multiple=multiple)
        # AFTER basic_ack!
        chan.tx_commit()  # commit sent messages and ack atomically!

    def _exc_headers(self, e: Exception) -> Dict[str, str]:
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

    def _pika_thread_cleanup(self) -> None:
        """
        Here in Pika thread when exiting
        """
        self._queue_kiss_of_death()  # wake main thread

    def _queue_kiss_of_death(self) -> None:
        """
        queue a sentinel to wake up other thread(s) on error
        """
        self._message_queue.put(None)

    def _retry(self, im: InputMessage, e: Exception) -> bool:
        """
        returns False if retries exhausted
        """
        oh = im.properties.headers  # old headers
        if oh:
            retries = oh.get(RETRIES_HDR, 0)
            if retries >= self.MAX_RETRIES:
                if isinstance(e, self.NO_QUARANTINE):
                    logger.info("discard %r", e)  # TEMP: change to debug
                else:
                    self._quarantine(im, e)
                return False  # retries exhausted
        else:
            retries = 0

        headers = self._exc_headers(e)
        headers[RETRIES_HDR] = retries + 1

        logger.info(f"retry #{retries} failed: {headers[EXCEPTION_HDR]}")

        # Queue message to -delay queue, which has no consumers, with
        # an expiration/TTL; when messages expire, they are routed
        # back to the -in queue via dead-letter-{exchange,routing-key}.

        # Would like exponential backoff (BASE << retries),
        # but https://www.rabbitmq.com/ttl.html says:
        #    When setting per-message TTL expired messages can queue
        #    up behind non-expired ones until the latter are consumed
        #    or expired.
        # ie; expiration only happens at head of queue (FIFO), and
        # all messages must have uniform expiration.
        #
        # The alternative, the delayed-message-exchange plugin has many
        # limitations (no clustering), and is not supported.
        expiration_ms_str = str(int(self.RETRY_DELAY_MINUTES * MS_PER_MINUTE))

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

    def set_requeue_delay_ms(self, ms: int) -> None:
        self.requeue_delay_str = str(int(ms))

    def _requeue(self, im: InputMessage) -> bool:
        """
        Requeue message to -fast queue, which has no consumers, with
        an expiration/TTL; when messages expire, they are routed
        back to the -in queue via dead-letter-{exchange,routing-key}.

        NOTE! requires -fast queue to be created (fast=True in pipeline.py)
        preserves all headers (does not zero retry count).

        Does NOT count number of times requeued!
        """
        props = BasicProperties(
            headers=im.properties.headers, expiration=self.requeue_delay_str
        )
        self._send_message(
            im.channel,
            im.body,
            DEFAULT_EXCHANGE,
            self.fast_queue_name,
            props,
        )
        return True  # requeued

    def message_queue_len(self) -> int:
        """
        NOTE! the underlying qsize method is described as "unreliable"
        USE ONLY FOR LOGGING/STATS!!
        """
        if self._message_queue:
            return self._message_queue.qsize()
        else:
            return 0

    def process_message(self, im: InputMessage) -> None:
        raise NotImplementedError("Worker.process_message not overridden")


class Producer(QApp):
    """
    QApp that produces messages
    """

    LOOP_SLEEP_TIME = 60.0
    MAX_QUEUE_LEN = 100000  # don't queue if (any) dest queue longer than this

    def __init__(self, process_name: str, descr: str):
        super().__init__(process_name, descr)
        self.sent_messages = 0

    def define_options(self, ap: argparse.ArgumentParser) -> None:
        super().define_options(ap)

        ap.add_argument(
            "--loop",
            action="store_true",
            default=False,
            help="Run until all files/input processed (sleeping if needed), else process one file/batch and quit.",
        )
        ap.add_argument(
            "--max-queue-len",
            type=int,
            default=self.MAX_QUEUE_LEN,
            help=f"Maximum queue length at which to send a new batch (default: {self.MAX_QUEUE_LEN})",
        )
        ap.add_argument(
            "--sleep",
            type=float,
            default=self.LOOP_SLEEP_TIME,
            help="Time to sleep between loop iterations (default: {self.LOOP_SLEEP_TIME})",
        )

    def check_output_queues(self) -> None:
        """
        snooze while output queue(s) have enough work;
        if in "try one and quit" (crontab) mode, just quit.
        """

        assert self.args
        max_queue = self.args.max_queue_len

        # get list of queues fed from this app's output exchange
        admin = self.admin_api()
        defns = admin.get_definitions()
        output_exchange = self.output_exchange_name
        queue_names = set(
            [
                binding["destination"]
                for binding in defns["bindings"]
                if binding["source"] == output_exchange
            ]
        )

        while True:
            # also wanted/used by scripts.rabbitmq-stats:
            queues = admin._api_get("/api/queues")
            for q in queues:
                name = q["name"]
                if name in queue_names:
                    ready = q["messages_ready"]
                    logger.debug("%s: ready %d", name, ready)
                    if ready > max_queue:
                        break
            else:
                # here when all queues short enough
                return

            if self.args.loop:
                logger.debug("sleeping until output queue(s) shorter")
                time.sleep(self.args.sleep)
            else:
                logger.info(
                    "queue(s) full enough: sent %d messages", self.sent_messages
                )
                sys.exit(0)


# story-related classes etc moved to storyapp.py
