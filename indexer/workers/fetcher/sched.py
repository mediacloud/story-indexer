"""
scheduler for threaded/queue fetcher

Scoreboard/issue/retire terminology comes from CPU instruction
scheduling.

hides details of data structures and locking.
"""

import logging
import math
import threading
import time
from enum import Enum
from typing import Any, Callable, Dict, List, NamedTuple, NoReturn, Optional, Type

from indexer.app import App

# try issue twice, with small, random sleep in between
SECOND_TRY = True

# number of seconds after start of last request to keep slot around
# (maintains request RTT)
SLOT_RECENT_MINUTES = 5

# exponential moving average coefficient for avg_seconds.
# (typ. used by TCP for RTT est)
# https://en.wikipedia.org/wiki/Exponential_smoothing
ALPHA = 0.25

logger = logging.getLogger(__name__)


# _could_ try and map Slots by IP address(es), since THAT gets us closer
# to the point (of not hammering a particular server);
#
#   BUT: Would have to deal with:
#   1. A particular FQDN may map to multiple IP addrs
#   2. The order of the IP addreses often changes between queries
#       (so doing a lookup first, then connecting by name
#        will likely give different first answers)
#   3. The ENTIRE SET might change if a CDN is involved!
#   4. Whether or not we're using IPv6 (if not, can ignore IPv6)
#   5. IP addresses can serve multiple domains
#   6. But domains served by shared servers (see #5)
#       might have disjoint IP addr sets.


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
        """
        return True if duration set, and time duration has passed
        """
        return self.duration is not None and self.elapsed() >= self.duration

    def __str__(self) -> str:
        """
        used in log messages!
        """
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


class ConnStatus(Enum):
    """
    connection status, reported to Slot.retire
    """

    NOCONN = -2
    BADURL = -1
    NODATA = 0
    DATA = 1


class Slot:
    """
    A slot for a single id (eg domain) within a ScoreBoard
    """

    def __init__(self, slot_id: str, sb: "ScoreBoard"):
        self.slot_id = slot_id  # ie; domain
        self.sb = sb

        self.active_count = 0
        self.last_issue = Timer(SLOT_RECENT_MINUTES * 60)
        # time since last error at this workplace
        self.last_conn_error = Timer(sb.conn_retry_seconds)
        self.avg_seconds = 0.0  # smoothed average
        self.issue_interval = 0.0

        # O(n) removal, only used for debug_info
        # unclear if using a Set would be better or not...
        self.active_threads: List[str] = []

    def _issue(self) -> IssueStatus:
        """
        return True if safe to issue (must call "retire" after)
        return False if cannot be issued now
        """
        # NOTE! Lock held: avoid logging!
        self.sb.big_lock.assert_held()

        # scrapy issue interval is avg_latency / concurrency
        # goal is to keep "concurrency" requests active
        if self.avg_seconds == 0:  # no running average yet.
            # issue up to concurrency limit requests:
            if self.active_count >= self.sb.target_concurrency:
                return IssueStatus.BUSY
        else:  # have running average of request times.
            elapsed = self.last_issue.elapsed()
            if elapsed < self.issue_interval:
                # WISH: return delta, for second try sleep time??
                return IssueStatus.BUSY

        # see if connection to domain failed "recently".
        # last test so that preference is short delay
        # (and hope an active fetch succeeds).
        if not self.last_conn_error.expired():
            return IssueStatus.SKIPPED

        self.active_count += 1
        self.last_issue.reset()

        self.active_threads.append(threading.current_thread().name)
        return IssueStatus.OK

    def retire(self, conn_status: ConnStatus, sec: float) -> None:
        """
        called when a fetch attempt has ended.
        """
        with self.sb.big_lock:
            # NOTE! Avoid logging while holding lock!!!

            assert self.active_count > 0
            self.active_count -= 1
            # remove on list is O(n), but n is small (concurrency limit)
            self.active_threads.remove(threading.current_thread().name)
            oavg = self.avg_seconds
            if conn_status == ConnStatus.NOCONN:
                self.last_conn_error.reset()
            elif conn_status == ConnStatus.DATA:
                if self.avg_seconds == 0:
                    self.avg_seconds = sec
                else:
                    # exponentially moving average (typ. used by TCP for RTT est)
                    # https://en.wikipedia.org/wiki/Exponential_smoothing
                    self.avg_seconds += (sec - self.avg_seconds) * ALPHA
            elif conn_status == ConnStatus.NODATA:  # got connection but no data
                # better to have some estimate of connection average time than none
                if self.avg_seconds == 0:
                    self.avg_seconds = sec

            if self.avg_seconds != oavg:
                self.issue_interval = self.avg_seconds / self.sb.target_concurrency

            # adjust scoreboard counters
            self.sb._slot_retired(self.active_count == 0)

    def _consider_removing(self) -> None:
        self.sb.big_lock.assert_held()  # PARANOIA
        if (
            self.active_count == 0
            and self.last_issue.expired()
            and self.last_conn_error.expired()
        ):
            self.sb._remove_slot(self.slot_id)


# status/value tuple: popular in GoLang
class IssueReturn(NamedTuple):
    status: IssueStatus
    slot: Optional[Slot]  # if status == OK


TS_IDLE = "idle"


class ThreadStatus:
    info: Optional[str]  # work info (URL or TS_IDLE)
    ts: float  # time.monotonic


class ScoreBoard:
    """
    keep score of active requests by "id" (str)
    """

    def __init__(
        self,
        app: App,  # for stats
        max_active: int,  # total concurrent active limit
        target_concurrency: int,  # max active with same id (domain)
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
        self.target_concurrency = target_concurrency
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

    def issue(self, slot_id: str, note: str) -> IssueReturn:
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

    def _slot_retired(self, slot_idle: bool) -> None:
        """
        here from slot.retired()
        """
        # NOTE! lock held: avoid logging
        self.big_lock.assert_held()
        assert self.active_fetches > 0
        self.active_fetches -= 1
        if slot_idle:
            assert self.active_slots > 0
            self.active_slots -= 1
            # XXX _consider_removing
        self._set_thread_status(TS_IDLE)

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

    def periodic(self, dump_slots: bool = False) -> None:
        """
        called periodically from main thread
        """
        with self.big_lock:
            # do this less frequently?
            for slot in list(self.slots.values()):
                slot._consider_removing()

            # avoid stats, logging with lock held!!!
            recent = len(self.slots)
            active_fetches = self.active_fetches
            active_slots = self.active_slots

            if dump_slots:
                self.debug_info_nolock()

        logger.info(
            "%d recently active; %d URLs in %d domains active",
            recent,
            active_fetches,
            active_slots,
        )

        self.app.gauge("active.recent", recent)
        self.app.gauge("active.fetches", active_fetches)
        self.app.gauge("active.slots", active_slots)

    def debug_info_nolock(self) -> None:
        """
        NOTE!!! called when lock attempt times out!
        dumps info without attempting to get lock!
        """
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
            if ts.info != TS_IDLE:
                logger.info("%s%s %.3f %s", name, have_lock, now - ts.ts, ts.info)
