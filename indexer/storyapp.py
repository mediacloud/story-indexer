"""
Story specific QApp stuff, split from worker.py
"""

# NOTE!!!! This file has been CAREFULLY coded to NOT assume consumers
# are recieving messages from exactly one channel/queue:
# * There is no channel global/member!!!
# * The code DOES assume there is only one Pika connection.
# * For code processing messages: Pika ops MUST be done from Pika thread

import argparse
import logging
import multiprocessing
import os
import queue
import random
import sys
import threading
import time
from typing import Dict, List, Optional, TypedDict
from urllib.parse import urlsplit

from mcmetadata.urls import NON_NEWS_DOMAINS
from pika import BasicProperties
from pika.adapters.blocking_connection import BlockingChannel

from indexer.app import AppProtocol, IntervalMixin
from indexer.story import BaseStory
from indexer.worker import (
    CONSUMER_TIMEOUT_SECONDS,
    DEFAULT_ROUTING_KEY,
    InputMessage,
    PikaThreadState,
    Producer,
    QApp,
    Worker,
)

# 10Mb- > 99.99% of pages should fit under this limit.
MAX_HTML_BYTES = int(os.environ.get("MAX_HTML_BYTES", 10000000))

logger = logging.getLogger(__name__)


def url_fqdn(url: str) -> str:
    """
    extract fully qualified domain name from url
    """
    # using urlsplit/SplitResult: parseurl calls spliturl and only
    # adds ";params" handling and this code only cares about netinfo
    surl = urlsplit(url, allow_fragments=False)
    hn = surl.hostname
    if not hn:
        raise ValueError("bad hostname")
    return hn.lower()


def non_news_fqdn(fqdn: str) -> bool:
    """
    check if a FQDN (fully qualified domain name, ie; DNS name)
    is (in) a domain embargoed as "non-news"

    maybe belongs in  mcmetadata??
    """
    # could be written as "any" on a comprehension:
    # looks like that's 15% slower in Python 3.10,
    # and harder to for me to... comprehend!
    fqdn = fqdn.lower()
    for nnd in NON_NEWS_DOMAINS:
        if fqdn == nnd or fqdn.endswith("." + nnd):
            return True
    return False


class StoryMixin(AppProtocol):
    """
    The place for Story-specific methods for both
    StoryProducers (output only) and Workers (in/out)
    """

    def define_options(self, ap: argparse.ArgumentParser) -> None:
        super().define_options(ap)
        self.test_file_prefix: str | None = None
        ap.add_argument(
            "--test-file-prefix",
            type=str,
            default=None,
            help="read and write WARC files instead of using queues",
        )

    def process_args(self) -> None:
        args = self.args
        assert args
        self.test_file_prefix = args.test_file_prefix
        if self.test_file_prefix:
            # Ouch! overriding a compile-time constant!
            self.AUTO_CONNECT = False
            # XXX set something to avoid rabbitmq-url check??
        super().process_args()  # after wacking AUTO_CONNECT

    def incr_stories(
        self, status: str, url: str, log_level: int = logging.INFO
    ) -> None:
        """
        Should be called exactly once for each Story processed.
        default level is INFO so logs track disposition of every story.
        """
        # All stats are prefixed by app name, so visible as
        # counters.mc.APP.stories.status_X or across all apps as
        # counters.mc.*.stories.status_X
        # (so try to use the same status strings across apps!)
        self.incr("stories", labels=[("status", status)])

        # could send to a sub-logger (__name__ + '.stories')
        logger.log(log_level, "%s: %s", status, url)

    def check_story_length(self, html: bytes, url: str) -> bool:
        """
        check HTML length:
        False return means a counter has been incremented and URL logged
        and the Story should be discarded.
        """
        if not html:
            self.incr_stories("no-html", url)
            return False

        if len(html) > MAX_HTML_BYTES:
            self.incr_stories("oversized", url)
            return False

        return True

    def check_story_url(self, url: str) -> bool:
        """
        check URL.
        False return means a counter has been incremented and URL logged,
        and the Story should be discarded.

        Ideally: call when queuing a new Story, and for each intermediate
        redirect URL while fetching.
        """
        if not url:
            self.incr_stories("no-url", url)
            return False

        # XXX check for over-sized URL??
        # rss-fetcher's default limit is 2048

        # using urlsplit/SplitResult: parseurl calls spliturl and only
        # adds ";params" handling and this code only cares about netinfo
        try:
            surl = urlsplit(url, allow_fragments=False)
        except ValueError:
            self.incr_stories("bad-url", url)
            return False

        hostname = surl.hostname
        if not hostname:
            self.incr_stories("no-host", url)
            return False

        # check for schema?

        if non_news_fqdn(hostname):
            self.incr_stories("non-news", url)
            return False

        return True


class StorySender:
    """
    Abstract class to hide how Story objects are sent
    """

    def send_story(
        self,
        story: BaseStory,
        exchange: Optional[str] = None,
        routing_key: str = DEFAULT_ROUTING_KEY,
        expiration_ms: Optional[int] = None,
    ) -> None: ...

    def flush(self) -> None:
        """
        noop unless batching
        """


class _BatchItem(TypedDict):
    story: BaseStory
    exchange: Optional[str]
    routing_key: str
    expiration_ms: Optional[int]


class RabbitMQStorySender(StorySender):
    """
    RabbitMQ based Story Sender -- hides channel.

    Stories must be sent on the channel they came in on to make
    transmission of new message and ACK of original atomic with
    tx_commit.
    """

    def __init__(self, app: QApp, channel: BlockingChannel, batch_size: int = 0):
        """
        If batch_size is greater than one, stories will be collected into
        batches and shuffled (order randomized) to try to maximize
        throughput in the queue-based fetcher; In this case the flush
        method MUST be called BEFORE each work unit (file, API
        response) of stories is marked as "done".
        """

        self.app = app
        self._channel = channel
        self._batch_size = batch_size
        self._batch: list[_BatchItem] = []

    def send_story(
        self,
        story: BaseStory,
        exchange: Optional[str] = None,
        routing_key: str = DEFAULT_ROUTING_KEY,
        expiration_ms: Optional[int] = None,
    ) -> None:
        if self._batch_size <= 1:
            # avoid overhead if not batching
            self._send(story, exchange, routing_key, expiration_ms)
        else:
            self._batch.append(
                _BatchItem(
                    story=story,
                    exchange=exchange,
                    routing_key=routing_key,
                    expiration_ms=expiration_ms,
                )
            )
            if len(self._batch) >= self._batch_size:
                self.flush()

    def _send(
        self,
        story: BaseStory,
        exchange: Optional[str],
        routing_key: str,
        expiration_ms: Optional[int],
    ) -> None:
        if expiration_ms is not None:
            props = BasicProperties(expiration=str(expiration_ms))
        else:
            props = None
        self.app._send_message(
            self._channel, story.dump(), exchange, routing_key, props
        )

    def flush(self) -> None:
        """
        shuffle (randomize order) of any saved stories and send them.

        This method MUST be called BEFORE each work unit (file, API
        response) of stories is marked as "done".
        """
        logger.info("shuffling %d stories", len(self._batch))
        random.shuffle(self._batch)  # randomize order in place
        for bi in self._batch:
            self._send(**bi)
        self._batch = []

        # block until all currently queued messages sent
        self.app.synchronize_with_pika_thread()


class ArchiveStorySender(StorySender):
    def __init__(self, prefix: str):
        # not used in production: avoid import unless used:
        from indexer.story_archive_writer import StoryArchiveWriter

        # will write all stories, regardless of dest queue to one archive.
        self.writer = StoryArchiveWriter(
            prefix=prefix,
            hostname="",  # ignored
            fqdn="",  # ignored
            serial=-1,  # ignore hostname, fqdn
            work_dir="",  # allow prefix to be absolute path
        )

    def send_story(
        self,
        story: BaseStory,
        exchange: Optional[str] = None,
        routing_key: str = DEFAULT_ROUTING_KEY,
        expiration_ms: Optional[int] = None,
    ) -> None:
        extra_metadata = {
            "queue_data": {
                "exchange": exchange,
                "routing_key": routing_key,
                # include expiration_ms?
            }
        }
        self.writer.write_story(story, extra_metadata, raise_errors=False)


class StoryProducer(StoryMixin, Producer):
    """
    Producer that queues new Story objects (w/o receiving any)
    """

    SAMPLE_PERCENT = 10.0  # for --sample-size

    def __init__(self, process_name: str, descr: str):
        super().__init__(process_name, descr)
        self.sender: Optional[StorySender] = None
        self.queued_stories = 0

    def define_options(self, ap: argparse.ArgumentParser) -> None:
        super().define_options(ap)

        ap.add_argument(
            "--dry-run",
            "-n",
            action="store_true",
            default=False,
            help="don't queue stories",
        )
        ap.add_argument(
            "--force",
            "-f",
            action="store_true",
            default=False,
            help="ignore tracking database (for test)",
        )
        ap.add_argument(
            "--max-stories",
            type=int,
            default=None,
            help="Number of stories to queue. Default (None) is 'all of them'",
        )
        ap.add_argument(
            "--random-sample",
            type=float,
            default=None,
            metavar="PERCENT",
            help="Percentage of stories to queue for testing (default: all)",
        )
        # _could_ be mutually exclusive with --max-count and --random-sample
        # instead, warnings output below
        ap.add_argument(
            "--sample-size",
            type=int,
            default=None,
            metavar="N",
            help=f"Implies --max-stories N --random-sample {self.SAMPLE_PERCENT}",
        )

    def process_args(self) -> None:
        super().process_args()
        args = self.args
        assert args

        if args.sample_size is not None:
            # could make options mutually exclusive, but would rather just complain:
            if args.max_stories is not None:
                logger.warning(
                    "--sample-size %s with --max-stories %s",
                    args.sample_size,
                    args.max_stories,
                )
            if args.max_stories is not None:
                logger.warning(
                    "--sample-size with --random-sample %s",
                    args.random_sample,
                )
            args.max_stories = args.sample_size
            args.random_sample = self.SAMPLE_PERCENT

    def check_output_queues(self) -> None:
        """
        snooze while output queues full,
        ignore if testing with WARC files!
        """

        assert self.args
        if self.args.test_file_prefix:
            return
        super().check_output_queues()

    def story_sender(self) -> StorySender:
        """
        MUST be called after qconnect, but before Pika thread running
        """
        if self.test_file_prefix:
            return ArchiveStorySender(f"{self.test_file_prefix}-out")

        assert self.connection
        # if pika thread running, it owns the connection:
        assert self._pika_thread is None
        batch_size = self._batch_size()
        return RabbitMQStorySender(self, self.connection.channel(), batch_size)

    def send_story(self, story: BaseStory, check_html: bool = False) -> None:
        assert self.args

        url = story.http_metadata().final_url or story.rss_entry().link or ""
        if not self.check_story_url(url):
            return  # logged and counted

        if check_html:
            html = story.raw_html().html or b""
            if not self.check_story_length(html, url):
                return  # logged and counted

        level = logging.INFO
        count = True
        if (
            self.args.random_sample is not None
            and random.random() * 100 > self.args.random_sample
        ):
            # here for randomly selecting URLs for testing
            status = "dropped"  # should not be seen in production!!!
            level = logging.DEBUG
            count = False  # don't count against limit!
        elif self.args.dry_run:
            status = "parsed"
        else:
            if self.sender is None:
                self.sender = self.story_sender()
            self.sender.send_story(story)
            status = "queuing"  # may be held for shuffling

        self.incr_stories(status, url, log_level=level)

        if not count:
            return

        self.queued_stories += 1
        if (
            self.args.max_stories is not None
            and self.queued_stories >= self.args.max_stories
        ):
            logger.info("%s %s stories; quitting", status, self.queued_stories)
            self.quit(0)

    def quit(self, status: int) -> None:
        sys.exit(status)

    def _batch_size(self) -> int:
        """
        return shuffle batch size to pass to RabbitMQStorySender
        """
        return 0  # no batching


class ShufflingStoryProducer(StoryProducer):
    """
    StoryProducer App that batches up and shufflles new stories to
    queue to try to maximize queue-based HTTP fetcher throughput.
    """

    # number of stories to batch and shuffle (zero to disable)
    # NOTE! Used as default rss-puller RSS_FETCHER_BATCH_SIZE.
    SHUFFLE_BATCH_SIZE = 2500

    def define_options(self, ap: argparse.ArgumentParser) -> None:
        super().define_options(ap)

        # will be zero in {arch,hist}-queuer (no reason to shuffle)
        if self.SHUFFLE_BATCH_SIZE > 0:
            default_shuffle_batch_size = int(
                os.environ.get("SHUFFLE_BATCH_SIZE", self.SHUFFLE_BATCH_SIZE)
            )
            ap.add_argument(
                "--shuffle-batch-size",
                type=int,
                default=default_shuffle_batch_size,
                help=f"number of stories to batch and shuffle before sending (default: {default_shuffle_batch_size})",
            )

    def flush_shuffle_batch(self) -> None:
        """
        shuffle and send accumulated stories, if any.

        MUST be called before work a unit of stories
        (a single file, or the results of an API call)
        is recorded as done.
        """
        if self.sender is not None:
            self.sender.flush()

    def quit(self, status: int) -> None:
        """
        here when queuing limit reached (when testing)
        """
        self.flush_shuffle_batch()
        super().quit(status)

    def _batch_size(self) -> int:
        """
        return shuffle batch size to pass to RabbitMQStorySender
        """
        # will be zero in {arch,hist}-queuer (no reason to shuffle)
        if self.SHUFFLE_BATCH_SIZE == 0:
            return 0  # no batching

        assert self.args
        return int(self.args.shuffle_batch_size)


class StoryWorker(StoryMixin, Worker):
    """
    Process Stories in Queue Messages
    """

    def __init__(self, process_name: str, descr: str):
        super().__init__(process_name, descr)

        # avoid needing to create senders on the fly.
        # Stories MUST be forwarded on the same channel they
        # came in on for transactions to quarantee atomic forward+ack.
        # NOTE: Currently only subscribing (chan.basic_consume)
        # on a single channel, but if you want to take input from
        # multiple queues, with different qos/prefetch values,
        # this would be necessary, so implement it now,
        # and avoid possible (if unlikely) surprise later.
        self.senders: Dict[BlockingChannel, StorySender] = {}

    def decode_story(self, im: InputMessage) -> BaseStory:
        story = BaseStory.load(im.body)
        assert isinstance(story, BaseStory)
        return story

    def _story_sender(self, chan: BlockingChannel) -> StorySender:
        sender = self.senders.get(chan)
        if not sender:
            sender = self.senders[chan] = RabbitMQStorySender(self, chan)
        return sender

    def process_message(self, im: InputMessage) -> None:
        sender = self._story_sender(im.channel)

        # raised exceptions will cause retry; quarantine immediately?
        story = self.decode_story(im)

        self.process_story(sender, story)

    def process_story(self, sender: StorySender, story: BaseStory) -> None:
        raise NotImplementedError("StoryWorker.process_story not overridden")

    def archive_main_loop(self) -> None:
        """
        here from main_loop when --test-file-prefix given.
        """
        # not used in production: avoid import unless used:
        from indexer.story_archive_writer import ARCHIVE_EXTENSION, StoryArchiveReader

        input_file = f"{self.test_file_prefix}-in{ARCHIVE_EXTENSION}"
        output_file = f"{self.test_file_prefix}-out"

        archive = StoryArchiveReader(open(input_file, "rb"))
        sender = ArchiveStorySender(output_file)
        stories = 0
        for story in archive.read_stories():
            # XXX inside try to handle errors for "retry"?!
            # (also QuarantineException and RetryException??)
            # or... force an InputMessage and call _process_one_message??
            # (would need to make ArchiveStorySender in _story_sender)
            self.process_story(sender, story)
            stories += 1
        logger.info("processed %d stories", stories)

    def main_loop(self) -> None:
        if self.test_file_prefix:
            return self.archive_main_loop()
        super().main_loop()


class BatchStoryWorker(StoryWorker):
    """
    A worker processing batches of stories
    (all stories consumed at once)
    """

    # Default values: just guesses, should be tuned.
    # Can be overridden in subclass.
    BATCH_SECONDS = 15 * 60  # time to wait for full batch
    BATCH_SIZE = 5000  # max batch size
    WORK_TIME = 5 * 60  # time to reserve for end_of_batch

    def define_options(self, ap: argparse.ArgumentParser) -> None:
        super().define_options(ap)

        ap.add_argument(
            "--batch-size",
            type=int,
            default=self.BATCH_SIZE,
            help=f"set batch size in stories (default {self.BATCH_SIZE})",
        )
        ap.add_argument(
            "--batch-seconds",
            type=int,
            default=self.BATCH_SECONDS,
            help=f"set batch timeout in seconds (default {self.BATCH_SECONDS})",
        )

    def process_args(self) -> None:
        super().process_args()

        # leave a minimum of one minute for processing!!!
        assert self.WORK_TIME < CONSUMER_TIMEOUT_SECONDS - 60
        batch_seconds_max = CONSUMER_TIMEOUT_SECONDS - self.WORK_TIME

        assert self.args
        if self.args.batch_seconds > batch_seconds_max:
            logger.error(
                "--batch-seconds %d too large (must be <= %d)",
                self.args.batch_seconds,
                batch_seconds_max,
            )
            sys.exit(1)

    def prefetch(self) -> int:
        # buffer exactly one full batch
        # (ACK on all messages delayed until batch processing complete)
        assert self.args
        return int(self.args.batch_size)

    def _process_messages(self) -> None:
        """
        Blocking loop for running Worker processing code on batches.
        Processes messages queued by _on_message (called from Pika thread).
        NOTE! Assumes all messages received on same channel!!
        """
        assert self.args
        batch_size = int(self.args.batch_size)
        batch_seconds = self.args.batch_seconds
        batch_deadline = 0.0  # deadline for starting batch processing
        batch_start_time = 0.0
        msg_number = 1
        msgs: List[InputMessage] = []

        logger.info("batch_size %d, batch_seconds %d", batch_size, batch_seconds)
        while self._state == PikaThreadState.RUNNING:
            while msg_number <= batch_size:  # msg_number is one-based
                if msg_number == 1:
                    logger.debug("waiting for first batch message")
                    im = self._message_queue.get()  # blocking
                    if im is None:
                        logger.info("_process_messages returning 1")
                        return
                    batch_start_time = time.monotonic()  # for logging

                    # base on when recieved from channel by Pika thread!!
                    batch_deadline = im.mtime + batch_seconds
                else:
                    try:
                        timeout = batch_deadline - time.monotonic()
                        if timeout <= 0:
                            break  # time is up! break batch loop
                        logger.debug(
                            "waiting %.3f seconds for batch message %d",
                            timeout,
                            msg_number,
                        )
                        im = self._message_queue.get(timeout=timeout)
                        if im is None:
                            logger.info("_process_messages returning 2")
                            return
                    except queue.Empty:
                        # exhausted the clock
                        break  # break batch loop

                if self._process_one_message(im):
                    # only keep & count if processed ok
                    msgs.append(im)
                    msg_number += 1
            # end of batch loop

            # here with at least one message and time expired,
            # or a full batch

            logger.info(
                "collected %d msg(s) in %.3f seconds",
                len(msgs),
                time.monotonic() - batch_start_time,
            )
            try:
                with self.timer("batch"):
                    self.end_of_batch()
            except Exception as e:
                # log as error, w/ exc_info=True?
                logger.info("end_of_batch caught %r", e)

                for im in msgs:
                    self._retry(im, e)
                self.incr("batches", labels=[("status", "retry")])

            # all msgs must be from same channel!!
            last_msg = msgs[-1]
            assert last_msg
            self._ack_and_commit(last_msg, multiple=True)
            msg_number = 1
            msgs = []

        sys.stdout.flush()  # for redirection, supervisord
        logger.info("_process_messages exiting")
        sys.exit(1)  # give error status so docker restarts

    def end_of_batch(self) -> None:
        raise NotImplementedError("BatchStoryWorker.end_of_batch not overridden")


# A StoryWorker that runs multiple threads processing Stories.  The
# subclass MUST use threading.Lock to ensure shared state is accessed
# atomically!  Would have liked this to have been a mixin, independent
# of Story object, but was too messy (hit on mypy MRO issue)


class MultiThreadStoryWorker(IntervalMixin, StoryWorker):
    # include thread name in log message format
    LOG_FORMAT = "thread"
    CPU_COUNT = multiprocessing.cpu_count()
    WORKER_THREADS_DEFAULT = CPU_COUNT

    def __init__(self, process_name: str, descr: str):
        super().__init__(process_name, descr)

        self.workers = self.WORKER_THREADS_DEFAULT
        self.threads: Dict[int, threading.Thread] = {}  # for debug
        # self.tls = threading.local()  # thread local storage object

        threading.main_thread().name = "Main"  # shorten name for logging
        self._worker_errors = False

    def define_options(self, ap: argparse.ArgumentParser) -> None:
        super().define_options(ap)
        ap.add_argument(
            "--worker-threads",
            "-W",
            type=int,
            default=self.workers,
            help=f"total active workers (default: {self.workers})",
        )

    def process_args(self) -> None:
        assert self.args
        self.workers = self.args.worker_threads
        assert self.workers > 0

        super().process_args()

        # logging configured by super().process_args()
        logger.info("%d workers", self.workers)

    def prefetch(self) -> int:
        # one to work on, and one ready for each worker thread
        return self.workers * 2

    def _worker_thread(self) -> None:
        """
        body for worker threads
        """
        self._process_messages()
        if self._state == PikaThreadState.RUNNING:
            logger.error("_worker_thread _process_messages returned")
        self._worker_errors = True

    def _start_worker_threads(self) -> None:
        for i in range(0, self.workers):
            t = threading.Thread(
                daemon=True,
                name=f"W{i:03d}",  # Wnnn same length as Pika/Main
                target=self._worker_thread,
            )
            t.start()
            self.threads[i] = t

    def _queue_kisses_of_death(self) -> None:
        """
        queue a "None" for each worker thread,
        ensuring workers wake up and knows the end is near.

        Called from main thread when _state != RUNNING
        or worker_errors is True
        """
        logger.info("queue_kisses_of_death")

        # wake up workers (in _process_messages)
        for i in range(0, self.workers):
            self._queue_kiss_of_death()
        # XXX join worker threads?

    def periodic(self) -> None:
        """
        main thread loops in calling periodic at an interval
        """
        logger.debug("periodic wakeup")

    def main_loop(self) -> None:
        try:
            self._start_worker_threads()
            while True:
                if self._state != PikaThreadState.RUNNING:
                    logger.info("_state %s", self._state)
                    break
                if self._worker_errors:
                    logger.info("_worker_errors")
                    break
                self.periodic()
                self.interval_sleep()
        finally:
            self._queue_kisses_of_death()
            # loop joining workers???

        # QApp.cleanup (called from App.main try/finally) calls
        # _stop_pika_thread.
        sys.exit(1)  # Workers never exit happy (always restart)
