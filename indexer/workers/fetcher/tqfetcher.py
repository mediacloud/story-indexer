# XXX handle missing URL schema?
# XXX Quarantine bad URLs?
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

When a Story can't be fetched because of connect rate or concurrency
limits, the Story is queued to a "fast delay" queue to avoid
bookkeeping complexity (and having an API that allows not ACKing a
message immediately).

In theory we have thirty minutes to ACK a message before RabbitMQ has
a fit (closes connection), so holding on to Stories that can't be
processed immediately is POSSIBLE, *BUT* the existing framework acks
the story after process_message/story (in _process_messages) and
handles exceptions for retry and quarantine.
"""

import argparse
import logging
import random
import signal
import time
from types import FrameType
from typing import NamedTuple, Optional

import requests
from requests.exceptions import RequestException

from indexer.app import run
from indexer.story import BaseStory
from indexer.storyapp import (
    MultiThreadStoryWorker,
    StorySender,
    non_news_fqdn,
    url_fqdn,
)
from indexer.worker import QuarantineException, RequeueException
from indexer.workers.fetcher.sched import IssueReturn, IssueStatus, ScoreBoard

# internal scheduling:
SLOT_REQUESTS = 4  # concurrent connections per domain

# Unless the input RSS entries are well mixed (and this would not be
# the case if the rss-fetcher queued RSS entries to us directly), RSS
# entries for the same domain will travel in packs/clumps/trains.  If
# the "fast" delay is too long, that allows only one URL to be issued
# each time the train passes.  So set the "fast" delay JUST long
# enough so they come back when intra-request issue interval has
# passed.  Note: the intra-request interval is based on response time
# divided by SLOT_REQUESTS, so is not a constant.
BUSY_DELAY_SECONDS = 2.0

# time cache server as bad after a connection failure
CONN_RETRY_MINUTES = 10

# requests timeouts:
CONNECT_SECONDS = 30.0
READ_SECONDS = 30.0  # for each read?

# HTTP parameters:
MAX_REDIRECTS = 30

# get from common place (also used by rss-fetcher)
USER_AGENT = "mediacloud bot for open academic research (+https://mediacloud.org)"

# scrapy default headers include: "Accept: text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
HEADERS = {"User-Agent": USER_AGENT}

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


class Fetcher(MultiThreadStoryWorker):
    WORKER_THREADS_DEFAULT = 200  # equiv to 20 fetchers, with 10 active fetches

    # Exceptions to discard instead of quarantine after repeated retries:
    # RequestException hierarchy includes bad URLs
    NO_QUARANTINE = (Retry, RequestException)

    def __init__(self, process_name: str, descr: str):
        super().__init__(process_name, descr)

        self.scoreboard: Optional[ScoreBoard] = None
        self.previous_fragment = ""

    def define_options(self, ap: argparse.ArgumentParser) -> None:
        super().define_options(ap)
        ap.add_argument(
            "--busy-delay-seconds",
            "-B",
            type=float,
            default=BUSY_DELAY_SECONDS,
            help=f"busy (fast) queue delay in seconds (default: {BUSY_DELAY_SECONDS})",
        )

        ap.add_argument(
            "--requests-per-domain",
            "-R",
            type=int,
            default=SLOT_REQUESTS,
            help=f"requests/domain goal (default: {SLOT_REQUESTS})",
        )

        ap.add_argument(
            "--no-second-try",
            default=False,
            action="store_true",
            help="don't try sleep/retry after busy",
        )

    def process_args(self) -> None:
        super().process_args()

        # Make sure cannot attempt a URL twice
        # using old status information:
        assert CONN_RETRY_MINUTES < self.RETRY_DELAY_MINUTES

        assert self.args
        self.scoreboard = ScoreBoard(
            self,
            self.workers,
            self.args.requests_per_domain,
            CONN_RETRY_MINUTES * 60,
        )

        self.busy_delay_seconds = self.args.busy_delay_seconds
        self.second_try = not self.args.no_second_try

        self.set_requeue_delay_ms(1000 * self.busy_delay_seconds)

        # enable debug dump on SIGUSR1
        def usr1_handler(sig: int, frame: Optional[FrameType]) -> None:
            if self.scoreboard:
                self.scoreboard.debug_info_nolock()

        signal.signal(signal.SIGUSR1, usr1_handler)

    def periodic(self) -> None:
        """
        called from main_loop
        """
        assert self.scoreboard
        with self.timer("status"):
            self.scoreboard.periodic()

    def fetch(self, sess: requests.Session, fqdn: str, url: str) -> FetchReturn:
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
                url = prepreq.url or ""
                prepreq = nextreq
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

    def try_issue(self, id: str, url: str) -> IssueReturn:
        # report time to issue: if this jumps up, it's
        # likely due to lock contention!
        assert self.scoreboard
        with self.timer("issue"):
            return self.scoreboard.issue(id, url)

    def process_story(self, sender: StorySender, story: BaseStory) -> None:
        """
        called from multiple worker threads!!

        This routine should call incr_stories EXACTLY once!
        """
        rss = story.rss_entry()

        url = rss.link
        if not url:
            return self.incr_stories("no-url", repr(url))
        assert isinstance(url, str)
        assert self.scoreboard is not None

        # XXX handle missing URL schema (default to http)??

        # NOTE!! fqdn isn't QUITE right: it means every
        # foobar.blogspot.com is treated as a separate server.
        # Really want to use IP address (see sched.py for more
        # gory details), but that would require resolving the
        # FQDN, and *THEN* using that address to make the HTTP
        # connection *AND* subsequent redirect fetches (if the
        # FQDN stays the same).

        # NOT using "domain" from RSS file because I originally
        # was planning to move the "issue" call inside the
        # redirect loop (getting clearance for each FQDN along the
        # chain), but if we ended up with a "busy", we'd have to
        # retry and start ALL over, or add a field to the Story
        # indicating the "next URL" to attempt to fetch, along
        # with a count of followed redirects.  AND, using
        # "canonical" domain means EVERYTHING inside a domain
        # looks to be one server (when that may not be the case).

        # BEFORE issue (discard without any delay)
        try:
            fqdn = url_fqdn(url)
        except (TypeError, ValueError):
            return self.incr_stories("badurl1", url)

        if non_news_fqdn(fqdn):
            # unlikely, if queuer does their job!
            return self.incr_stories("non-news", url)

        ir = self.try_issue(fqdn, url)
        if ir.status == IssueStatus.BUSY and self.second_try:
            # Not inside "issue" to avoid messing up timing stats!
            # All messages in the fast queue get the same delay,
            # so introduce some randomness here.
            time.sleep(self.busy_delay_seconds * random.random())
            ir = self.try_issue(fqdn, url)

        if ir.slot is None:  # could not be issued
            if ir.status == IssueStatus.SKIPPED:
                # Skipped due to recent connection error: Treat as if
                # we saw an error as well (incrementing retry count on the Story)
                # rather than waiting 30 seconds for connection to fail again.
                # After a failure the scheduler remembers the slot as failing
                # for CONN_RETRY_MINUTES (not currently tunable via an option).
                self.incr_stories("skipped", url)
                raise Retry("skipped due to recent connection failure")
            else:
                # here when "busy", due to one of (in order of likelihood):
                # 1. per-fqdn connect interval not reached
                # 2. per-fqdn currency limit reached
                # 3. total concurrecy limit reached.
                # requeue in short-delay queue, without counting as retry.
                self.incr_stories("busy", url, log_level=logging.DEBUG)
                raise RequeueException("busy")  # does not increment retry count

        # here with slot marked active *MUST* call ir.slot.retire!!!!
        t0 = time.monotonic()
        with self.timer("fetch"):
            # log starting URL
            logger.info("fetch %s", url)

            sess = requests.Session()
            try:  # call retire on exit
                fret = self.fetch(sess, fqdn, url)
                got_connection = True
            except (
                requests.exceptions.InvalidSchema,
                requests.exceptions.MissingSchema,
                requests.exceptions.InvalidURL,
            ) as exc:
                logger.info("%s: %r", url, exc)
                self.incr_stories("badurl2", url)
                # bad URL, did not attempt connection, so don't mark domain as down!
                got_connection = True  # checked in finally
                return  # discard: do not pass go, do not collect $200!
            except Exception:
                self.incr_stories("noconn", url)
                got_connection = False  # checked in finally
                raise  # re-raised for retry counting
            finally:
                # decrement slot active_count!
                # remember if connection attempt failed.
                ir.slot.retire(got_connection, time.monotonic() - t0)
                sess.close()

        resp = fret.resp  # requests.Response
        if resp is None:  # NOTE!!! non-200 responses are Falsy?!
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

        logger.info("length %d content-type %s", lcontent, ct)  # XXX report ms?

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
