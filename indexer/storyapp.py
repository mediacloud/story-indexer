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
import os
import queue
import sys
import time
from typing import Any, Dict, List, Optional
from urllib.parse import SplitResult, urlsplit

from mcmetadata.urls import NON_NEWS_DOMAINS
from pika.adapters.blocking_connection import BlockingChannel

from indexer.app import AppProtocol
from indexer.story import BaseStory
from indexer.worker import (
    CONSUMER_TIMEOUT_SECONDS,
    DEFAULT_ROUTING_KEY,
    InputMessage,
    QApp,
    Worker,
)

# 10Mb- > 99.99% of pages should fit under this limit.
MAX_HTML_BYTES = int(os.environ.get("MAX_HTML_BYTES", 10000000))

logger = logging.getLogger(__name__)


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
    object to hide channel.

    Stories must be sent on the channel they came in on to make
    transmission of new message and ACK of original atomic with
    tx_commit.
    """

    def __init__(self, app: QApp, channel: BlockingChannel):
        self.app = app
        self._channel = channel

    def send_story(
        self,
        story: BaseStory,
        exchange: Optional[str] = None,
        routing_key: str = DEFAULT_ROUTING_KEY,
    ) -> None:
        self.app._send_message(self._channel, story.dump(), exchange, routing_key)


class StoryProducer(StoryMixin, QApp):
    """
    QApp that queues new Story objects
    """

    def story_sender(self) -> StorySender:
        """
        MUST be called after qconnect, before Pika thread running
        """
        assert self.connection
        assert self._pika_thread is None
        return StorySender(self, self.connection.channel())


class StoryWorker(StoryMixin, Worker):
    """
    Process Stories in Queue Messages
    """

    def __init__(self, process_name: str, descr: str):
        super().__init__(process_name, descr)

        # avoid needing to create senders on the fly
        # for each incomming message.
        self.senders: Dict[BlockingChannel, StorySender] = {}

    def process_message(self, im: InputMessage) -> None:
        chan = im.channel
        if chan in self.senders:
            sender = self.senders[chan]
        else:
            sender = self.senders[chan] = StorySender(self, chan)

        # raised exceptions will cause retry; quarantine immediately?
        story = BaseStory.load(im.body)

        self.process_story(sender, story)

    def process_story(self, sender: StorySender, story: BaseStory) -> None:
        raise NotImplementedError("StoryWorker.process_story not overridden")


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

    def _qos(self, chan: BlockingChannel) -> None:
        """
        set "prefetch" limit: distributes messages among workers
        processes, limits the number of unacked messages queued
        """
        assert self.args
        chan.basic_qos(prefetch_count=self.args.batch_size)

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
        while self._running:
            while msg_number <= batch_size:  # msg_number is one-based
                if msg_number == 1:
                    logger.info("waiting for first batch message")  # move to debug?
                    im = self._message_queue.get()  # blocking
                    batch_start_time = time.monotonic()  # for logging

                    # base on when recieved from channel by Pika thread!!
                    batch_deadline = im.mtime + batch_seconds
                else:
                    try:
                        timeout = batch_deadline - time.monotonic()
                        if timeout <= 0:
                            break  # time is up! break batch loop
                        logger.info(  # move to debug?
                            "waiting %.3f seconds for batch message %d",
                            timeout,
                            msg_number,
                        )
                        im = self._message_queue.get(timeout=timeout)
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
