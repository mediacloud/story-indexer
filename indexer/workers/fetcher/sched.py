"""
scheduler for threaded/queue fetcher

Scoreboard/issue/retire terminology comes from CPU instruction
scheduling.

hides details of data structures and locking.
"""

import logging
import math
import random
import threading
import time
from enum import Enum
from typing import Any, Callable, Dict, List, NamedTuple, NoReturn, Optional, Type

from indexer.app import App

# try issue twice, with small, random sleep in between
SECOND_TRY = False

# EXPERIMENT: try Scrapy latency test (keep max_per_slot requests
# active).  This uses max_per_slot divided by average_request_time as
# the issue interval, and (except when starting up) ignores
# max_per_slot as hard limit, and always ignores min_seconds.
# Primary advantage is for busy feeds with low latency.
SCRAPY_LATENCY = True

# exponential moving average coefficient for avg_seconds.
# (typ. used by TCP for RTT est)
# https://en.wikipedia.org/wiki/Exponential_smoothing
# scrapy uses simple mean (ALPHA = 0.5)
ALPHA = 0.25

logger = logging.getLogger(__name__)


# _could_ try and map Slots by IP address(es), since THAT gets us closer
# to the point (of not hammering a particular server),
#
#   BUT: Would have to deal with:
#   1. A particular FQDN may map to multiple IP addrs
#   2. The order of the IP addreses might well change between queries
#   3. The ENTIRE SET might change if a CDN is involved!
#   4. Whether or not we're using IPv6 (if not, can ignore IPv6)
#   5. IP addresses can serve multiple domains
#   6. But domains in #5 might have disjoint IP addr sets.


class LockError(RuntimeError):
    """
    base class for locking exceptions
    """


class LockNotHeldError(LockError):
    """
    lock was not held, when should be
    """


class LockHeldError(LockError):
    """
    lock was held, when should not be
    """


class LockTimeout(LockError):
    """
    took too long to get lock
    """


class Lock:
    """
    wrapper for threading.Lock
    keeps track of thread that holds lock
    """

    def __init__(self, name: str, error_handler: Callable[[], None]):
        self.name = str
        self._lock = threading.Lock()
        self._error_handler = error_handler
        self._owner: Optional[threading.Thread] = None

    def held(self) -> bool:
        """
        return True if current thread already holds lock
        """
        return self._owner is threading.current_thread()

    def _raise(self, exc_type: Type[LockError]) -> NoReturn:
        """
        here on fatal locking error
        call error handler and raise exception
        """
        self._error_handler()
        raise exc_type(self.name)

    def assert_held(self) -> None:
        if not self.held():
            self._raise(LockNotHeldError)

    def assert_not_held(self) -> None:
        if self.held():
            self._raise(LockHeldError)

    def __enter__(self) -> Any:
        self.assert_not_held()  # non-recursive lock
        if not self._lock.acquire(timeout=120):
            self._raise(LockTimeout)
        self._owner = threading.current_thread()

    def __exit__(self, type: Any, value: Any, traceback: Any) -> None:
        self._owner = None
        self._lock.release()


_NEVER = 0.0


class Timer:
    """
    measure intervals; doesn't start ticking until reset called.
    """

    def __init__(self, duration: Optional[float]) -> None:
        """
        lock is container object lock (for asserts)
        """
        self.last = _NEVER
        self.duration = duration

    def elapsed(self) -> float:
        """
        return seconds since last "reset"
        """
        if self.last == _NEVER:
            return math.inf
        return time.monotonic() - self.last

    def reset(self) -> None:
        """
        (re)start from zero
        """
        self.last = time.monotonic()

    def expired(self) -> bool:
        return self.duration is not None and self.elapsed() >= self.duration

    def __str__(self) -> str:
        if self.last == _NEVER:
            return "not set"
        if self.expired():
            return "expired"
        return f"{self.elapsed():.3f}"


class IssueStatus(Enum):
    """
    return value from Slot._issue
    """

    OK = 0  # slot assigned
    BUSY = 1  # too many fetches active or too soon
    SKIPPED = 2  # recent connection error


class Slot:
    """
    A slot for a single id (eg domain) within a ScoreBoard
    """

    def __init__(self, slot_id: str, sb: "ScoreBoard"):
        self.slot_id = slot_id  # ie; domain
        self.sb = sb

        self.active_count = 0
        self.active_threads: List[str] = []
        self.last_issue = Timer(sb.min_seconds)
        # time since last error at this workplace
        self.last_conn_error = Timer(sb.conn_retry_seconds)
        self.avg_seconds = 0.0  # smoothed average

        # XXX *COULD* keep list of active threads
        # (instead of active count)

    def _issue(self) -> IssueStatus:
        """
        return True if safe to issue (must call "retire" after)
        return False if cannot be issued now
        """
        self.sb.big_lock.assert_held()
        if SCRAPY_LATENCY:
            # scrapy issue interval is avg_latency / concurrency
            # goal is to keep "concurrency" requests active
            if self.avg_seconds == 0:
                if self.active_count >= self.sb.max_per_slot:
                    return IssueStatus.BUSY
            else:  # have avg_seconds:
                elapsed = self.last_issue.elapsed()
                goal = self.avg_seconds / self.sb.max_per_slot
                logger.debug("%s: elapsed %.3f goal %.3f", self.slot_id, elapsed, goal)
                if elapsed < goal:
                    return IssueStatus.BUSY
        else:
            if self.active_count >= self.sb.max_per_slot:
                return IssueStatus.BUSY

            if not self.last_issue.expired():
                # issued recently
                return IssueStatus.BUSY

        # see if connection to domain failed "recently".
        # last test so that preference is short delay
        # (and hope an active fetch succeeds).
        # XXX reconsider this to reduce fast traffic???
        if not self.last_conn_error.expired():
            return IssueStatus.SKIPPED

        self.active_count += 1
        self.last_issue.reset()

        self.active_threads.append(threading.current_thread().name)
        return IssueStatus.OK

    def retire(self, got_connection: bool, sec: float) -> None:
        """
        called when a fetch attempt has ended.
        got_connection is False when the connection attempt failed.
        """
        with self.sb.big_lock:
            assert self.active_count > 0
            self.active_count -= 1
            self.active_threads.remove(threading.current_thread().name)  # O(n)
            if not got_connection:
                self.last_conn_error.reset()
            else:
                if self.avg_seconds == 0:
                    self.avg_seconds = sec
                else:
                    # exponentially moving average (typ. used by TCP for RTT est)
                    # https://en.wikipedia.org/wiki/Exponential_smoothing
                    oavg = self.avg_seconds
                    self.avg_seconds += (sec - self.avg_seconds) * ALPHA
                    logger.info(
                        "%s: sec %.3f old %.3f new %.3f",
                        self.slot_id,
                        sec,
                        oavg,
                        self.avg_seconds,
                    )

            # adjust scoreboard counters
            self.sb._slot_retired(self.active_count == 0)

            # consider removing idle slot if no bans in place
            self._consider_removing()

    def _consider_removing(self) -> None:
        self.sb.big_lock.assert_held()  # PARANOIA
        if (
            self.active_count == 0
            and self.last_issue.expired()
            and self.last_conn_error.expired()
        ):
            logger.debug("removing idle slot %s", self.slot_id)
            self.sb._remove_slot(self.slot_id)


# status/value tuple: popular in GoLang
class IssueReturn(NamedTuple):
    status: IssueStatus
    slot: Optional[Slot]  # if status == OK


class ThreadStatus:
    info: Optional[str]  # work info (URL or "idle")
    ts: float  # time.monotonic


class ScoreBoard:
    """
    keep score of active requests by "id" (str)
    """

    def __init__(
        self,
        app: App,  # for stats
        max_active: int,  # total concurrent active limit
        max_per_slot: int,  # max active with same id (domain)
        min_seconds: float,  # seconds between issues for slot
        conn_retry_seconds: float,  # seconds before connect retry
    ):
        self.app = app
        # single lock, rather than one each for scoreboard, active count,
        # and each slot.  Time spent with lock held should be small,
        # and lock ordering issues likely to make code complex and fragile!

        self.big_lock = Lock(
            "big_lock", self.debug_info_nolock
        )  # covers ALL variables!
        self.max_active = max_active
        self.max_per_slot = max_per_slot
        self.min_seconds = min_seconds
        self.conn_retry_seconds = conn_retry_seconds
        self.slots: Dict[str, Slot] = {}  # map "id" (domain) to Slot
        self.active_fetches = 0
        self.active_slots = 0

        # map thread name to ThreadStatus
        self.thread_status: Dict[str, ThreadStatus] = {}

    def _get_slot(self, slot_id: str) -> Slot:
        # _COULD_ try to use IP addresses to map to slots, BUT would
        # have to deal with multiple addrs per domain name and
        # possibility of non-overlapping sets from different fqdns
        self.big_lock.assert_held()  # PARANOIA
        slot = self.slots.get(slot_id, None)
        if not slot:
            slot = self.slots[slot_id] = Slot(slot_id, self)
        return slot

    def _remove_slot(self, slot_id: str) -> None:
        del self.slots[slot_id]

    def _issue(self, slot_id: str, note: str) -> IssueReturn:
        with self.big_lock:
            if self.active_fetches < self.max_active:
                slot = self._get_slot(slot_id)
                status = slot._issue()
                if status == IssueStatus.OK:
                    # *MUST* call slot.retire() when done
                    if slot.active_count == 1:  # was idle
                        self.active_slots += 1
                    self.active_fetches += 1
                    self._set_thread_status(note)  # full URL
                    return IssueReturn(status, slot)
            else:
                status = IssueStatus.BUSY
        return IssueReturn(status, None)

    def issue(self, slot_id: str, note: str) -> IssueReturn:
        ir = self._issue(slot_id, note)
        if ir.status == IssueStatus.BUSY and SECOND_TRY:
            # experiment: wait a few seconds and try again,
            # both to increase liklihood of issue,
            # and to de-clump the queue:
            # can't have different intervals in delay queue, so do it here:
            time.sleep(5.0 * random.random() + 1)
            ir = self._issue(slot_id, note)
        return ir

    def _slot_retired(self, idle: bool) -> None:
        """
        here from slot.retired()
        """
        self.big_lock.assert_held()
        assert self.active_fetches > 0
        self.active_fetches -= 1
        if idle:
            assert self.active_slots > 0
            self.active_slots -= 1
            # XXX _consider_removing
        self._set_thread_status("idle")

    def _set_thread_status(self, info: str) -> None:
        """
        save status for debug info
        """
        # no lock assert: only current thread can write it's own status item
        index = threading.current_thread().name
        ts = self.thread_status.get(index, None)
        if not ts:
            ts = self.thread_status[index] = ThreadStatus()
        ts.info = info
        ts.ts = time.monotonic()

    def periodic(self) -> None:
        """
        called periodically from main thread
        """
        with self.big_lock:
            # do this less frequently?
            for slot in list(self.slots.values()):
                slot._consider_removing()

            recent = len(self.slots)
            logger.info(
                "%d recently active; %d URLs in %d domains active",
                recent,
                self.active_fetches,
                self.active_slots,
            )

            self.app.gauge("active.recent", recent)
            self.app.gauge("active.fetches", self.active_fetches)
            self.app.gauge("active.slots", self.active_slots)

    def debug_info_nolock(self) -> None:
        """
        NOTE!!! called when lock attempt times out!
        dumps info without attempting to get lock!
        """
        logger.info(
            "%d slots; %d URLs in %d domains active",
            len(self.slots),
            self.active_fetches,
            self.active_slots,
        )

        for domain, slot in list(self.slots.items()):
            logger.info(
                "%s: %s last issue: %s last err: %s",
                domain,
                ",".join(slot.active_threads),
                slot.last_issue,
                slot.last_conn_error,
            )

        # here without lock, so grab before examining:
        lock_owner_thread = self.big_lock._owner
        if lock_owner_thread:
            lock_owner_name = lock_owner_thread.name
        else:
            lock_owner_name = "NONE"

        now = time.monotonic()
        for name, ts in list(self.thread_status.items()):
            if name == lock_owner_name:
                have_lock = " *LOCK*"
            else:
                have_lock = ""
            # by definition debug info, but only on request/error
            # so try to avoid having it filtered out:
            logger.info("%s%s %.3f %s", name, have_lock, now - ts.ts, ts.info)
