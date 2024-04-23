"""
Keep track of queued work files.

Isolates implementation of record keeping for Queuers.
Would prefer something replicated/durable (S3, AWS SimpleDB, ES)
see https://github.com/mediacloud/story-indexer/issues/203
"""

import logging
import os
import sqlite3
import time
from enum import Enum
from typing import Any, Optional, Tuple, Type

from indexer.app import AppException
from indexer.path import app_data_dir

# basename assumes URLs and local paths both use "/":
assert os.path.sep == "/"

OLD_SECONDS = 24 * 60 * 60  # age before eligible for cleanup

logger = logging.getLogger(__name__)


class TrackerException(AppException):
    """
    base class for FileTracker exceptions
    """


class TrackerNotStartable(TrackerException):
    """
    thrown when file is in some state other than NOT_STARTED
    """


class TrackerLocked(TrackerException):
    """
    raised internally when a (local file based) tracker is locked
    and should be retried in response to _open_and_lock.

    After retries, may be raised by (with) context entry.
    """


class FileStatus(Enum):
    """
    file status: values (except NOT_STARTED) may be thrown
    as Exception by FileTracker __init__.
    All values are instances of FileStatus
    """

    NOT_STARTED = 1
    STARTED = 2  # processing started
    FINISHED = 3


class FileTracker:
    """
    class to track input files
    """

    def __init__(self, app_name: str, fname: str, cleanup: bool):
        self.app_name = app_name
        self.full_name = fname  # saved
        self.fname = self.basename(fname)
        self.cleanup = cleanup

    def basename(self, fname: str) -> str:
        """
        trim URL/pathname to base file name (no directory or .gz)

        All input file types have date-distinct canonical file names:
        warc: prefix-YYYYMMDDhhmmss-{serial}-{hostname}.warc.gz
        hist csv: YYYY_MM_DD.csv
        rss: mc-YYYY-MM-DD.rss.gz
        """
        # NOTE!!! This assumes local filesystem uses "/"
        # as path separator, just like URLs!!!
        base = os.path.basename(fname)

        # look for "extension" suffixes associated with gzip and remove
        if "." in base:
            # get final "extension"
            prefix, ext = base.rsplit(".", 1)
            if ext.lower() in ("gz", "gzip"):
                return prefix
        return base

    def __enter__(self) -> "FileTracker":
        """
        atomically check if NOT_STARTED, and if so, mark STARTED.
        subclasses should raise TrackerNotStartable(status.name)
        if status != NOT_STARTED
        """
        self._start_if_available()
        return self

    def __exit__(self, type: Any, value: Any, traceback: Any) -> None:
        if traceback:
            self._set_status(FileStatus.NOT_STARTED)  # here on error
        else:
            self._set_status(FileStatus.FINISHED)

    def _start_if_available(self) -> None:
        raise NotImplementedError("_start_if_available not overridden")

    def _set_status(self, status: FileStatus) -> None:
        raise NotImplementedError("_set_status not overridden")


class DummyFileTracker(FileTracker):
    """
    file tracker that never says no (for debug/test)
    """

    def _start_if_available(self) -> None:
        pass

    def _set_status(self, status: FileStatus) -> None:
        pass


class LocalFileTracker(FileTracker):
    """
    a file tracker that uses local storage (with a single lock & retries)
    Local concurrent access is safe.
    NFS access at your own risk/funeral!!!
    """

    FILE_EXT: str

    def __init__(self, app_name: str, fname: str, cleanup: bool):
        super().__init__(app_name, fname, cleanup)
        work_dir = app_data_dir(app_name)
        self._db_path = os.path.join(work_dir, f"file-tracker.{self.FILE_EXT}")

    def _start_if_available(self) -> None:
        try:
            self._retry_open_and_lock()
            status, ts = self._db_get_status_time()
            logger.debug("_start_if_available %s %d", status, ts)
            if status == FileStatus.NOT_STARTED.name or (
                self.cleanup
                and status == FileStatus.STARTED.name
                and (time.time() - ts) > OLD_SECONDS
            ):
                self._set_status(FileStatus.STARTED)
            else:
                raise TrackerNotStartable(status)
        finally:
            self._db_close()

    def _set_status(self, status: FileStatus) -> None:
        logger.info("%s now %s", self.fname, status.name)
        try:
            self._retry_open_and_lock()
            self._db_set_status_time(status, time.time())
        finally:
            self._db_close()

    def _retry_open_and_lock(self) -> None:
        """
        GDBM and SQLite3 can both raise exceptions when DB locked.
        Will re-raise TrackerLocked when tired of retrying.
        """
        sec = 1 / 16
        while True:
            try:
                self._db_open_and_lock()
                return
            except TrackerLocked:
                sec *= 2
                if sec > 60:
                    raise
                logger.debug("_retry_open_and_lock sleeping %g", sec)
                time.sleep(sec)

    def _db_open_and_lock(self) -> None:
        """
        Either returns with DB open and locked, or raises TrackerLocked.
        May be called with database already open/locked.
        """
        raise NotImplementedError("_db_open_and_lock")

    def _db_get_status_time(self) -> Tuple[str, int]:
        raise NotImplementedError("_get_status_time")

    def _db_set_status_time(self, status: FileStatus, ts: float) -> None:
        raise NotImplementedError("_db_set_status_time not overridden")

    def _db_close(self) -> None:
        raise NotImplementedError("_db_close")


if False:
    import dbm.gnu
    import errno

    class GDBMFileTracker(LocalFileTracker):
        """
        Temporarily retained as an alternate example
        No longer holds file lock while processing,
        but SQLite file is easier to examine, backup and modify!
        """

        FILE_EXT = "db"
        _DEF_VAL = FileStatus.NOT_STARTED.name.encode() + ",0"

        def __init__(self, app_name: str, fname: str, cleanup: bool):
            self._dbm: Optional[dbm.gnu._gdbm] = None
            super().__init__(app_name, fname, cleanup)

        def _open_and_lock(self) -> None:
            if self._dbm:
                return
            try:
                self._dbm = dbm.gnu.open(self._db_path, "c")
            except OSError as e:
                if e.errno == errno.EAGAIN:
                    raise TrackerLocked(self._db_path)
                raise

        def _db_get_status_time(self) -> Tuple[str, int]:
            assert self._dbm
            val = self._dbm.get(self.fname, self._DEF_VAL).decode()
            status, ts = val.split(",")
            return (status, int(ts))

        def _db_set_status_time(self, status: FileStatus, ts: float) -> None:
            assert self._dbm
            if status == FileStatus.NOT_STARTED:
                del self._dbm[self.fname]
            else:
                status_string = f"{status.name},{int(ts)}"
                self._dbm[self.fname] = status_string.encode()

        def _db_close(self) -> None:
            if self._dbm:
                self._dbm.close()
                self._dbm = None


_SQL3_CREATE_TABLE = (
    "CREATE TABLE IF NOT EXISTS "
    "files (name TEXT PRIMARY KEY, status TEXT NOT NULL, ts INTEGER) "
    "WITHOUT ROWID"
)


class SQLite3FileTracker(LocalFileTracker):
    """
    easier to view, back up and mody than GDBM
    SHOULD ONLY BE USED LOCALLY (all Queuers on one node)!!!
    """

    FILE_EXT = "sqlite3"

    def __init__(self, app_name: str, fname: str, cleanup: bool):
        self._conn: Optional[sqlite3.Connection] = None
        super().__init__(app_name, fname, cleanup)

    def _db_open_and_lock(self) -> None:
        if self._conn:
            return
        self._conn = sqlite3.connect(self._db_path)
        self._conn.execute(_SQL3_CREATE_TABLE)
        try:
            self._conn.execute("BEGIN IMMEDIATE")
        except sqlite3.OperationalError:
            raise TrackerLocked(self._db_path)

    def _db_set_status_time(self, status: FileStatus, ts: float) -> None:
        assert self._conn
        if status == FileStatus.NOT_STARTED:
            self._conn.execute("DELETE FROM files WHERE name = ?", (self.fname,))
        else:
            self._conn.execute(
                "INSERT OR REPLACE INTO files VALUES (?,?,?)",
                (self.fname, status.name, int(ts)),
            )
        self._conn.execute("COMMIT")

    def _db_get_status_time(self) -> Tuple[str, int]:
        assert self._conn
        cursor = self._conn.execute(
            "SELECT status, ts FROM files WHERE name = ?", (self.fname,)
        )
        row = cursor.fetchone()
        if row is None:
            return FileStatus.NOT_STARTED.name, 0
        return str(row[0]), int(row[1])

    def _db_close(self) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None


def get_tracker(app_name: str, fname: str, force: bool, cleanup: bool) -> FileTracker:
    """
    force: process all files, do not update database
    cleanup: treat old STARTED entries as NOT_STARTED
    """
    # complex decision process:
    cls: Type[FileTracker]
    if force:
        cls = DummyFileTracker  # always says yes
    else:
        # _could_ look for existing local file.
        cls = SQLite3FileTracker
    return cls(app_name, fname, cleanup)
