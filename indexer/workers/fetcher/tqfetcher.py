"""
"Threaded Queue Fetcher" using RabbitMQ

NOTE! As of 2023 with CPython, the Global Interpreter Lock (GIL) means
that threads don't give greater concurrency than async/coroutines
(only one thread/task runs at a time), BUT PEP703 describes work in
progress to eliminate the GIL, over time, enabling the code to run on
multiple cores.

Regardless, most of the time of an active fetch request is likely to
be waiting for I/O (due to network/server latency), or CPU bound in
SSL processing, neither of which requires holding the GIL.

We have thirty minutes to ACK a message before RabbitMQ has a fit
(closes connection), so:

* All scheduling done in Pika thread, as messages delivered by Pika
  * As messages come to _on_input_message, the next time a fetch could
    be issued is assigned by calling scoreboard.get_delay
  * If the delay would mean the fetch would start more than BUSY_DELAY_MINUTES
    in the future, the message is requeued to the "-fast" delay queue
    (and will return in BUSY_DELAY_MINUTES).
  * If connections to the server have failed "recently", behave as if
    this connection failed, and requeue the story for retry.
  * Else call pika_connection.call_later w/ the entire InputMessage and
    a callback to queue the InputMessage to the work queue (_message_queue)
    and the InputMessage will be picked up by a worker thread and passed
    to process_story()
"""

# To find all stories_incr label names:
# egrep 'FetchReturn\(|GetIdReturn\(|incr_stor' tqfetcher.py

import argparse
import logging
import os
import signal
import sys
import time
from types import FrameType
from typing import NamedTuple, Optional

import requests
from mcmetadata.webpages import MEDIA_CLOUD_USER_AGENT
from pika.adapters.blocking_connection import BlockingChannel
from requests.exceptions import RequestException

from indexer.app import run
from indexer.story import BaseStory
from indexer.storyapp import (
    MultiThreadStoryWorker,
    StorySender,
    non_news_fqdn,
    url_fqdn,
)
from indexer.worker import InputMessage, QuarantineException, RequeueException
from indexer.workers.fetcher.sched import (
    DELAY_LONG,
    DELAY_SKIP,
    ConnStatus,
    ScoreBoard,
    StartStatus,
)

TARGET_CONCURRENCY = 10  # scrapy fetcher AUTOTHROTTLE_TARGET_CONCURRENCY

# minimum interval between initiation of requests to a site
# lower values increase chance of concurrent connections to
# sites that respond quickly.
MIN_INTERVAL_SECONDS = 0.5

# default delay time for "fast" queue, and max time to delay stories
# w/ call_later.  Large values allow more requests to be delayed, so
# keeping it small, hopefully breaking up clumps.
BUSY_DELAY_MINUTES = 2

# time to cache server as down after a connection failure
CONN_RETRY_MINUTES = 10

# requests timeouts:
CONNECT_SECONDS = 30.0
READ_SECONDS = 30.0  # for each read?

# HTTP parameters:
MAX_REDIRECTS = 30

# scrapy default headers include: "Accept: text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
HEADERS = {"User-Agent": MEDIA_CLOUD_USER_AGENT}

# HHTP response codes to retry
# (all others cause URL to be discarded)
RETRY_HTTP_CODES = set(
    [
        408,  # Request Timeout
        429,  # Too Many Requests
        500,  # Internal Server Error
        # 501 is "Not Implemented"
        502,  # Bad Gateway
        503,  # Service Unavailable
        504,  # Gateway Timeout
        # Cloudflare:
        522,  # Connection timed out
        524,  # A Timeout Occurred
    ]
)

# distinct counters for these HTTP response codes:
SEPARATE_COUNTS = set([403, 404, 429])

logger = logging.getLogger("fetcher")  # avoid __main__


class Retry(Exception):
    """
    Exception to throw for explicit retries
    (included in NO_QUARANTINE, so never quarantined)
    """


class FetchReturn(NamedTuple):
    resp: Optional[requests.Response]

    # only valid if resp is None:
    counter: str
    quarantine: bool


class GetIdReturn(NamedTuple):
    status: str  # counter name if != "ok"
    url: str
    id: str


class Fetcher(MultiThreadStoryWorker):
    # Exceptions to discard instead of quarantine after repeated retries:
    # RequestException hierarchy includes bad URLs
    NO_QUARANTINE = (Retry, RequestException)

    def __init__(self, process_name: str, descr: str):
        super().__init__(process_name, descr)

        self.scoreboard: Optional[ScoreBoard] = None
        self.prefetch = 0

    def define_options(self, ap: argparse.ArgumentParser) -> None:
        super().define_options(ap)
        ap.add_argument(
            "--busy-delay-minutes",
            type=float,
            default=BUSY_DELAY_MINUTES,
            help=f"busy (fast) queue delay in minutes (default: {BUSY_DELAY_MINUTES})",
        )

        ap.add_argument(
            "--conn-retry-minutes",
            type=float,
            default=CONN_RETRY_MINUTES,
            help=f"minutes to cache connection failure (default: {CONN_RETRY_MINUTES})",
        )

        ap.add_argument(
            "--min-interval-seconds",
            type=float,
            default=MIN_INTERVAL_SECONDS,
            help=f"minimum connection interval in seconds (default: {MIN_INTERVAL_SECONDS})",
        )

        ap.add_argument(
            "--target-concurrency",
            type=int,
            default=TARGET_CONCURRENCY,
            help=f"goal for concurrent requests/fqdn (default: {TARGET_CONCURRENCY})",
        )

        ap.add_argument(
            "--dump-slots",
            default=False,
            action="store_true",
            help="dump slot info once a minute",
        )

    def process_args(self) -> None:
        super().process_args()
        assert self.args

        # Make sure cannot attempt a URL twice using old status information:
        if self.args.conn_retry_minutes >= self.RETRY_DELAY_MINUTES:
            logger.error(
                "conn-retry-minutes must be less than %d", self.RETRY_DELAY_MINUTES
            )
            sys.exit(1)

        self.busy_delay_seconds = self.args.busy_delay_minutes * 60

        self.scoreboard = ScoreBoard(
            target_concurrency=self.args.target_concurrency,
            max_delay_seconds=self.busy_delay_seconds,
            conn_retry_seconds=self.args.conn_retry_minutes * 60,
            min_interval_seconds=MIN_INTERVAL_SECONDS,
        )

        self.set_requeue_delay_ms(1000 * self.busy_delay_seconds)

        # enable debug dump on SIGUSR1
        def usr1_handler(sig: int, frame: Optional[FrameType]) -> None:
            if self.scoreboard:
                self.scoreboard.debug_info_nolock()

        signal.signal(signal.SIGUSR1, usr1_handler)

    def _qos(self, chan: BlockingChannel) -> None:
        """
        set "prefetch" limit, the number of unacked messages
        RabbitMQ will send us at any time.

        Active requests, ready messages waiting in _message_queue,
        and delayed (call_later) should total to the prefetch limit.

        NOTE!!! Failure to send an ACK to RabbitMQ for
        CONSUMER_TIMEOUT_SECONDS for ANY message will cause
        RabbitMQ to close the connection!!!  So the estimate
        MUST be prssimistic!!
        """
        # Want to avoid very large numbers of requests in the "ready"
        # state (in message queue), since there is no inter-request
        # delay enforced once requests land there.
        self.prefetch = self.workers * 2
        logger.info("prefetch %d", self.prefetch)
        chan.basic_qos(prefetch_count=self.prefetch)

    def periodic(self) -> None:
        """
        called from main_loop
        """
        assert self.scoreboard
        assert self.args

        with self.timer("status"):
            stats = self.scoreboard.periodic(self.args.dump_slots)

        ready = self.message_queue_len()  # ready for workers
        # delayed counts not adjusted until "start" called,
        # so subtract messages in message_queue:
        delayed = stats.delayed - ready

        load_avgs = os.getloadavg()

        # when input queue non-empty, first three should total to self.prefetch
        logger.info(
            "%d active, %d ready, %d delayed, for %d sites, %d recent; lavg %.2f",
            stats.active_fetches,
            ready,
            delayed,
            stats.active_slots,
            stats.slots,
            load_avgs[0],
        )

        def requests(label: str, count: int) -> None:
            self.gauge("requests", count, labels=[("status", label)])

        requests("active", stats.active_fetches)
        requests("ready", ready)
        requests("delayed", delayed)

        # above three should total to prefetch:
        self.gauge("prefetch", self.prefetch)

        self.gauge("slots.recent", stats.slots)
        self.gauge("slots.active", stats.active_slots)

    def fetch(self, sess: requests.Session, url: str) -> FetchReturn:
        """
        perform HTTP get, tracking redirects looking for non-news domains

        Raises RequestException on connection and HTTP errors.
        Returns FetchReturn NamedTuple for uniform handling of counts.
        """
        redirects = 0

        # prepare initial request:
        request = requests.Request("GET", url, headers=HEADERS)
        prepreq = sess.prepare_request(request)
        while True:  # loop processing redirects
            with self.timer("get"):  # time each HTTP get
                # NOTE! maybe catch/retry malformed URLs from redirects??
                resp = sess.send(
                    prepreq,
                    allow_redirects=False,
                    timeout=(CONNECT_SECONDS, READ_SECONDS),
                    verify=False,  # raises connection rate
                )

            if not resp.is_redirect:
                # here with a non-redirect HTTP response:
                # it could be an HTTP error!

                # XXX report redirect count as a statsd "timing"? histogram??
                # with resp non-null, other args should be ignored
                return FetchReturn(resp, "SNH", False)

            # here with redirect:
            nextreq = resp.next  # PreparedRequest | None
            if nextreq:
                prepreq = nextreq
                url = prepreq.url or ""
            else:
                url = ""

            if not url:
                return FetchReturn(None, "badredir", False)

            redirects += 1
            if redirects >= MAX_REDIRECTS:
                return FetchReturn(None, "maxredir", False)

            try:
                fqdn = url_fqdn(url)
            except (TypeError, ValueError):
                return FetchReturn(None, "badredir2", False)

            # NOTE: adding a counter here would count each story fetch attempt more than once

            logger.info("redirect (%d) => %s", resp.status_code, url)
            if non_news_fqdn(fqdn):
                return FetchReturn(None, "non-news2", False)  # in redirect

        # end infinite redirect loop

    def get_id(self, story: BaseStory) -> GetIdReturn:
        """
        This function determines what stories are treated as from
        the same "server".

        NOT using "domain" from RSS file because I originally
        was planning to move the "issue" call inside the
        redirect loop (getting clearance for each FQDN along the
        chain), but if we ended up with a "busy", we'd have to
        retry and start ALL over, or add a field to the Story
        indicating the "next URL" to attempt to fetch, along
        with a count of followed redirects.  AND, using
        "canonical" domain means EVERYTHING inside a domain
        looks to be one server (when that may not be the case).

        *COULD* look up addresses, sort them, and pick the lowest or
        highest?!  this would avoid hitting single servers that handle
        many thing.dom.ain names hard, but incurrs overhead (and
        unless the id is stashed in the story object would require
        multiple DNS lookups: initial Pika thread dispatch, in worker
        thread for "start" call, and again for actual connection.
        Hopefully the result is cached nearby, but it would still incurr
        latency for due to system calls, network delay etc.
        """
        rss = story.rss_entry()

        url = rss.link
        if not url:
            return GetIdReturn("no-url", repr(url), "bad")

        assert isinstance(url, str)

        # BEFORE issue (discard without locking/delay)
        try:
            fqdn = url_fqdn(url)
        except (TypeError, ValueError):
            return GetIdReturn("badurl1", url, fqdn)

        if non_news_fqdn(fqdn):
            # unlikely, if queuer does their job!
            return GetIdReturn("non-news", url, fqdn)

        return GetIdReturn("ok", url, fqdn)

    def _on_input_message(self, im: InputMessage) -> None:
        """
        YIKES!! override a basic Worker method!!!
        Performs an additional decode of serialized Story!
        NOTE! Not covered by exception catching for retry!!!
        MUST ack and commit before returning!!!

        pre-processes incomming stories, delaying them
        (using the Pika "channel.call_later" method)
        so that they're queued to the worker pool
        with suitable inter-request delays for each server.

        DOES NOT INCREMENT STORY COUNTER!!!
        (perhaps have a different counter??)
        """
        assert self.scoreboard is not None
        assert self.connection is not None

        try:
            story = self.decode_story(im)

            status, url, id = self.get_id(story)
            if status != "ok":
                self.incr_stories(status, url)
                self._pika_ack_and_commit(im)  # drop (ack without requeuing)
                return

            with self.timer("get_delay"):
                delay = self.scoreboard.get_delay(id)

            logger.info("%s: delay %.3f", url, delay)
            if delay >= 0:
                # NOTE! Using pika connection.call_later because it's available.
                # "put" does not need to be run in the Pika thread, and the
                # delay _could_ be managed in another thread.
                def put() -> None:
                    # _put_message queue is the normal "_on_input_message" handler
                    logger.debug("put #%s", im.method.delivery_tag)
                    self._put_message_queue(im)

                # holding message, will be acked when processed
                if delay == 0:
                    # enforce SOME kind of rate limit?
                    # see comments in Slot._get_delay()
                    put()
                else:
                    logger.debug("delay #%s", im.method.delivery_tag)
                    self.connection.call_later(delay, put)
                return
            elif delay == DELAY_SKIP:
                raise Retry("skipped due to recent connection failure")
            elif delay == DELAY_LONG:
                self._requeue(im)
            else:
                raise Retry(f"unknown delay {delay}")
        except Exception as exc:
            self._retry(im, exc)
        self._pika_ack_and_commit(im)

    def process_story(self, sender: StorySender, story: BaseStory) -> None:
        """
        called in a worker thread
        retry/quarantine exceptions handled normally
        """
        istatus, url, id = self.get_id(story)
        if istatus != "ok":
            logger.warning("get_id returned ('%s', '%s')", istatus, id)
            self.incr_stories(istatus, id)
            return

        assert self.scoreboard is not None
        start_status, slot = self.scoreboard.start(id, url)
        if start_status == StartStatus.SKIP:
            self.incr_stories("skipped2", url)
            raise Retry("skipped due to recent connection failure")
        elif start_status == StartStatus.BUSY:
            self.incr_stories("busy", url)
            raise RequeueException("busy")
        elif start_status != StartStatus.OK:
            logger.warning("start status %s: %s", start_status, url)
            raise Retry(f"start status {start_status}")
        assert slot is not None

        # ***NOTE*** here with slot marked active *MUST* call slot.finish!!!!
        t0 = time.monotonic()
        with self.timer("fetch"):
            # log starting URL
            logger.info("fetch %s", url)

            sess = requests.Session()
            try:  # call retire on exit
                fret = self.fetch(sess, url)
                if fret.resp and fret.resp.status_code == 200:
                    conn_status = ConnStatus.DATA
                else:
                    conn_status = ConnStatus.NODATA
            except (
                requests.exceptions.InvalidSchema,
                requests.exceptions.MissingSchema,
                requests.exceptions.InvalidURL,
            ) as exc:
                logger.info("%s: %r", url, exc)
                self.incr_stories("badurl2", url)
                # bad URL, did not attempt connection, so don't mark domain as down!
                conn_status = ConnStatus.BADURL  # used in finally
                return  # discard: do not pass go, do not collect $200!
            except Exception:
                self.incr_stories("noconn", url)
                conn_status = ConnStatus.NOCONN  # used in finally
                raise  # re-raised for retry counting
            finally:
                # ALWAYS: report slot now idle!!
                # jumps in timing indicate lock contention!!
                with self.timer("finish"):
                    # keep track of connection success, latency
                    slot.finish(conn_status, time.monotonic() - t0)
                sess.close()

        resp = fret.resp  # requests.Response
        if resp is None:
            self.incr_stories(fret.counter, url)
            if fret.quarantine:
                raise QuarantineException(fret.counter)
            return

        status = resp.status_code
        if status != 200:
            if status in SEPARATE_COUNTS:
                counter = f"http-{status}"
            else:
                counter = f"http-{status//100}xx"

            msg = f"HTTP {status} {resp.reason}"
            if status in RETRY_HTTP_CODES:
                self.incr_stories(counter, url)
                raise Retry(msg)
            else:
                return self.incr_stories(counter, msg)
        # here with status == 200
        content = resp.content  # bytes
        lcontent = len(content)
        ct = resp.headers.get("content-type", "")

        logger.info("length %d content-type %s", lcontent, ct)

        # Scrapy skipped non-text documents: need to filter them out
        if not resp.encoding and not (
            ct.startswith("text/")
            or ct.startswith("application/xhtml")
            or ct.startswith("application/vnd.wap.xhtml+xml")
            or ct.startswith("application/xml")
        ):
            # other XML types handled by scrapy: application/atom+xml
            # application/rdf+xml application/rss+xml.
            # Logging the rejected content-types here
            # so that they can be seen in the log files:
            return self.incr_stories("not-text", url)

        if not self.check_story_length(content, url):
            return  # logged and counted

        final_url = resp.url
        with self.timer("queue"):
            with story.http_metadata() as hmd:
                hmd.response_code = status
                hmd.final_url = final_url
                hmd.encoding = resp.encoding  # from content-type header
                hmd.fetch_timestamp = time.time()

            with story.raw_html() as rh:
                rh.html = content
                rh.encoding = resp.encoding

            sender.send_story(story)
        self.incr_stories("success", final_url)


if __name__ == "__main__":
    run(Fetcher, "fetcher", "HTTP Page Fetcher")
