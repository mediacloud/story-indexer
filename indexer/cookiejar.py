"""
API for storing an opaque cookie string

Abstracted to class in a module in case there is ever
a "properties" database table (ES index?) for such things!
"""

import logging
import os

from indexer.path import app_data_dir

logger = logging.getLogger(__name__)


class CookieJar:
    """
    read/write a file that keeps an opaque cookie string

    MUST call last and write in pairs
    (NEVER assume last thing written is the contents of the file)!
    """

    def __init__(self, app_name: str, cookie_name: str, force: bool, empty: str = ""):
        self.path = os.path.join(app_data_dir(app_name), cookie_name)
        self.force = force  # don't write file, reads back last value
        self.old_stats: os.stat_result | None = None
        self.empty = empty
        self._last_value = empty

    def read(self) -> str:
        """
        return saved cookie, or self.empty if none set
        """
        if self.force:
            return self._last_value

        try:
            with open(self.path, "r") as f:
                value = f.read()
                self.old_stats = os.fstat(f.fileno())
        except FileNotFoundError:
            value = self.empty
            logger.info("%s not found: using %r", self.path, value)
            self.old_stats = None
        return value

    def write(self, value: str) -> None:
        """
        write cookie to file.
        Do NOT assume file will contain last written contents!!!
        """
        if self.force:
            self._last_value = value
            return

        if self.old_stats:
            old_mtime = self.old_stats.st_mtime
            self.old_stats = None
            new_stat = os.stat(self.path)
            if new_stat.st_mtime != old_mtime:
                logger.warning(
                    "%s: modification time changed since read (not writing)", self.path
                )
                return

        tmp = self.path + ".tmp"
        with open(tmp, "w") as f:
            f.write(value)
        # try to survive unremovable .prev file
        try:
            if os.path.exists(self.path):
                os.rename(self.path, self.path + ".prev")
        except OSError:
            pass
        os.rename(tmp, self.path)
