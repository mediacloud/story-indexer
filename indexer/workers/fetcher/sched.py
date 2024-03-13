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

# number of seconds after start of last request to keep idle slot around
# if no active/delayed requests and no errors (maintains request RTT)
SLOT_RECENT_MINUTES = 5

# exponential moving average coefficient for avg_seconds.
# (typ. used by TCP for RTT est)
# https://en.wikipedia.org/wiki/Exponential_smoothing
ALPHA = 0.25

logger = logging.getLogger(__name__)


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


class Alarm:
    """
    time until a future event (born expired)
    """

    def __init__(self) -> None:
        self.time = 0.0

    def set(self, delay: float) -> None:
        """
        if unexpired, extend expiration by delay seconds;
        else set expiration to delay seconds in future
        """
        now = time.monotonic()
        if self.time > now:
            self.time += delay
        else:
            self.time = now + delay

    def delay(self) -> float:
        """
        return seconds until alarm expires
        """
        return self.time - time.monotonic()

    def __str__(self) -> str:
        """
        used in log messages!
        """
        delay = self.delay()
        if delay >= 0:
            return "%.2f" % delay
        return "READY"


class ConnStatus(Enum):
    """
    connection status, reported to Slot.retire
    """

    NOCONN = -2
    BADURL = -1
    NODATA = 0
    DATA = 1


class StartStatus(Enum):
    """
    status from start()
    """

    OK = 1  # ok to start
    SKIP = 2  # recently reported down
    BUSY = 3  # currently busy


DELAY_SKIP = -1.0  # recent attempt failed
DELAY_LONG = -2.0  # delay to long to hold


class Slot:
    """
    A slot for a single id (eg domain) within a ScoreBoard
    """

    def __init__(self, slot_id: str, sb: "ScoreBoard"):
        self.slot_id = slot_id  # ie; domain
        self.sb = sb

        # time since last error at this workplace:
        self.last_conn_error = Timer(sb.conn_retry_seconds)

        self.avg_seconds = 0.0  # smoothed average
        self.issue_interval = sb.min_interval_seconds

        self.last_start = Timer(SLOT_RECENT_MINUTES)

        self.next_issue = Alarm()
        self.delayed = 0
        self.active = 0

        # O(n) removal, only used for debug_info
        # unclear if using a Set would be better or not...
        self.active_threads: List[str] = []

    def _get_delay(self) -> float:
        """
        return delay in seconds until fetch can begin.
        or value < 0 (DELAY_XXX)
        """

        # NOTE! Lock held: avoid logging!
        self.sb.big_lock.assert_held()

        # see if connection to domain failed "recently".
        if not self.last_conn_error.expired():
            return DELAY_SKIP

        delay = self.next_issue.delay()
        if delay > self.sb.max_delay_seconds:
            return DELAY_LONG

        if delay < 0:
            delay = 0.0  # never issued or past due

        self.next_issue.set(self.issue_interval)
        self.delayed += 1
        self.sb.delayed += 1
        return delay

    def _start(self) -> StartStatus:
        """
        Here in a worker thread, convert from delayed to active
        returns False if connection failed recently
        """
        # NOTE! Lock held: avoid logging!
        self.sb.big_lock.assert_held()

        self.delayed -= 1
        self.sb.delayed -= 1

        # see if connection to domain failed "recently".
        # last test so that preference is short delay
        # (and hope an active fetch succeeds).
        if not self.last_conn_error.expired():
            return StartStatus.SKIP

        if self.active >= self.sb.target_concurrency:
            return StartStatus.BUSY

        self.active += 1
        self.last_start.reset()

        self.active_threads.append(threading.current_thread().name)
        return StartStatus.OK

    def finish(self, conn_status: ConnStatus, sec: float) -> None:
        """
        called when a fetch attempt has ended.
        """
        with self.sb.big_lock:
            # NOTE! Avoid logging while holding lock!!!

            assert self.active > 0
            self.active -= 1
            # remove on list is O(n), but n is small (concurrency target)
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
                interval = self.avg_seconds / self.sb.target_concurrency
                if interval < self.sb.min_interval_seconds:
                    interval = self.sb.min_interval_seconds
                self.issue_interval = interval

            # adjust scoreboard counters
            self.sb._slot_finished(self.active == 0)

    def _consider_removing(self) -> None:
        self.sb.big_lock.assert_held()  # PARANOIA
        if (
            self.active == 0
            and self.delayed == 0
            and self.last_start.expired()
            and self.last_conn_error.expired()
        ):
            self.sb._remove_slot(self.slot_id)


TS_IDLE = "idle"


class ThreadStatus:
    info: Optional[str]  # work info (URL or TS_IDLE)
    ts: float  # time.monotonic


class StartRet(NamedTuple):
    """
    return value from start()
    """

    status: StartStatus
    slot: Optional[Slot]


class Stats(NamedTuple):
    """
    statistics returned by periodic()
    """

    slots: int
    active_fetches: int
    active_slots: int
    delayed: int


class ScoreBoard:
    """
    keep score of active requests by "id" (str)
    """

    # arguments changed often in development,
    # so all must be passed by keyword
    def __init__(
        self,
        *,
        target_concurrency: int,  # max active with same id (domain)
        max_delay_seconds: float,  # max time to hold
        conn_retry_seconds: float,  # seconds before connect retry
        min_interval_seconds: float,
    ):
        # single lock, rather than one each for scoreboard, active count,
        # and each slot.  Time spent with lock held should be small,
        # and lock ordering issues likely to make code complex and fragile!

        self.big_lock = Lock(
            "big_lock", self.debug_info_nolock
        )  # covers ALL variables!
        self.target_concurrency = target_concurrency
        self.max_delay_seconds = max_delay_seconds
        self.conn_retry_seconds = conn_retry_seconds
        self.min_interval_seconds = min_interval_seconds

        self.slots: Dict[str, Slot] = {}  # map "id" (domain) to Slot
        self.active_slots = 0
        self.active_fetches = 0
        self.delayed = 0

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

    def get_delay(self, slot_id: str) -> float:
        """
        called when story first picked up from message queue.
        return time to hold message before starting (delayed counts incremented)
        if too long (more than max_delay_seconds), returns -1
        """
        with self.big_lock:
            slot = self._get_slot(slot_id)
            return slot._get_delay()

    def start(self, slot_id: str, note: str) -> StartRet:
        """
        here from worker thread, after delay (if any)
        """
        with self.big_lock:
            slot = self._get_slot(slot_id)
            status = slot._start()
            if status == StartStatus.OK:
                # *MUST* call slot.finished() when done
                if slot.active == 1:  # was idle
                    self.active_slots += 1
                self.active_fetches += 1
                self._set_thread_status(note)  # full URL
                return StartRet(status, slot)
            return StartRet(status, None)

    def _slot_finished(self, slot_idle: bool) -> None:
        """
        here from slot.finished()
        """
        # NOTE! lock held: avoid logging
        self.big_lock.assert_held()
        assert self.active_fetches > 0
        self.active_fetches -= 1
        if slot_idle:
            assert self.active_slots > 0
            self.active_slots -= 1
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

    def periodic(self, dump_slots: bool = False) -> Stats:
        """
        called periodically from main thread.
        NOTE!! dump_slots logs with lock held!!!!
        Use only for debug!
        """
        with self.big_lock:
            # do this less frequently?
            for slot in list(self.slots.values()):
                slot._consider_removing()

            if dump_slots:  # NOTE!!! logs with lock held!!!
                self.debug_info_nolock()

            return Stats(
                slots=len(self.slots),
                active_fetches=self.active_fetches,
                active_slots=self.active_slots,
                delayed=self.delayed,
            )

    def debug_info_nolock(self) -> None:
        """
        NOTE!!! called when lock attempt times out!
        dumps info without attempting to get lock!
        """
        for domain, slot in list(self.slots.items()):
            logger.info(
                "%s: %s avg %.3f, %da, %dd, last issue: %s last err: %s next: %s",
                domain,
                ",".join(slot.active_threads),
                slot.avg_seconds,
                slot.active,
                slot.delayed,
                slot.last_start,
                slot.last_conn_error,
                slot.next_issue,
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
