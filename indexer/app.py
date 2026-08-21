"""
Base class for command line applications
"""

import argparse
import datetime as dt
import logging
import os
import sys
import time
import urllib.parse
from types import TracebackType
from typing import TYPE_CHECKING, Any, List, Optional, Tuple, TypeAlias

# PyPI
import statsd  # depends on stubs/statsd.pyi

# from mediacloud/system-dev-ops:
from mc_logging.logger import SendtoSocketWrapper, log_to_sink

# story-indexer:
from indexer import sentry

Labels: TypeAlias = List[
    Tuple[str, Any]
]  # optional labels/values for a statistic report

# breadcrumbs are small JSON messages sent to summarize message paths
BreadCrumb: TypeAlias = dict[str, int | str | bool | None]  # simple JSON dict

LEVEL_DEST = "log_level"  # args entry name!
LEVELS = [level.lower() for level in logging._nameToLevel.keys()]
LOGGER_LEVEL_SEP = ":"
TAGS = False  # get from env?? graphite >= 1.1.0 tags

logger = logging.getLogger(__name__)


class AppException(RuntimeError):
    """
    App class Exceptions
    """


# Dicts of log formats, indexed by App.LOG_FORMAT

# see https://docs.python.org/3/library/logging.html#logrecord-attributes
# for options in log formats.

# formats for stderr; indexed by App.LOG_THREAD_ID
STDERR_FORMATS: dict[bool, str] = {
    False: "%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    True: "%(asctime)s | %(levelname)s | %(name)s | %(threadName)s | %(message)s",
}

# syslog format now configured in mc_logging.logger


class App:
    """
    Base class for command line applications (ie; Worker)
    """

    LOG_THREAD_ID: bool = False

    def __init__(self, process_name: str, descr: str):
        # override of process_name allow alternate versions of pipeline
        # (ie; processing historical data) from different queues
        self.process_name = os.environ.get("PROCESS_NAME", process_name)
        self.descr = descr
        self.args: Optional[argparse.Namespace] = None  # set by main
        self._statsd: Optional[statsd.StatsdClient] = None

    def define_options(self, ap: argparse.ArgumentParser) -> None:
        """
        subclass if additional options/argument needed.
        subclass methods _SHOULD_ call super() method BEFORE adding options
        for consistent option ordering.
        """
        # logging, a subset from rss-fetcher fetcher.logargparse:
        ap.add_argument(
            "--debug",
            "-d",
            action="store_const",
            const="DEBUG",
            dest=LEVEL_DEST,
            help="set default logging level to 'DEBUG'",
        )
        ap.add_argument(
            "--quiet",
            "-q",
            action="store_const",
            const="WARNING",
            dest=LEVEL_DEST,
            help="set default logging level to 'WARNING'",
        )

        # UGH! requires positional args! Implement as an Action class?
        ap.add_argument(
            "--list-loggers",
            action="store_true",
            dest="list_loggers",
            help="list all logger names and exit",
        )

        log_level = os.getenv("LOG_LEVEL", "INFO")
        ap.add_argument(
            "--log-level",
            "-l",
            action="store",
            choices=LEVELS,
            dest=LEVEL_DEST,
            default=log_level,
            help=f"set default logging level to LEVEL (default {log_level})",
        )

        # set specific logger verbosity:
        ap.add_argument(
            "--logger-level",
            "-L",
            action="append",
            dest="logger_level",
            help=(
                "set LOGGER (see --list-loggers) "
                "verbosity to LEVEL (see --log-level)"
            ),
            metavar=f"LOGGER{LOGGER_LEVEL_SEP}LEVEL",
        )

    def process_args(self) -> None:
        """
        process arguments after parsing command line, but before main_loop.
        subclasses can override this method to perform initialization based
        on command line arguments, but MUST call super().process_args() FIRST
        (so that logging is initialized first)
        """

        if self.args is None:
            raise AppException("self.args not set")

        if self.args.list_loggers:
            for name in sorted(logging.root.manager.loggerDict):
                print(name)
            sys.exit(0)

        level = getattr(self.args, LEVEL_DEST)
        if level is None:
            level = "INFO"
        else:
            level = level.upper()

        log_thread_id = self.LOG_THREAD_ID

        # NOTE! Levels applied to root logger, so effect
        # both stderr handler created by basicConfig.
        # _COULD_ apply to just stderr *handler* and
        # send everything to syslog handler.
        logging.basicConfig(format=STDERR_FORMATS[log_thread_id], level=level)

        if self.args.logger_level:
            for ll in self.args.logger_level:
                logger_name, level = ll.split(LOGGER_LEVEL_SEP, 1)
                # XXX check logger_name in logging.root.manager.loggerDict??
                # XXX check level.upper() in LEVELS?
                logging.getLogger(logger_name).setLevel(level.upper())

        log_to_sink(self.process_name, log_thread_id=log_thread_id)

        ################ logging now enabled

        # end process_args

    ################ stats reporting

    def _stats_init(self) -> None:
        """
        one-time init for statistics
        """
        # FYI: STATSD_URL is set by dokku-graphite plugin
        statsd_url = os.getenv("STATSD_URL", None)
        if not statsd_url:
            logger.info("STATSD_URL not set")
            return

        parsed_url = urllib.parse.urlparse(statsd_url)
        if parsed_url.scheme != "statsd":
            logger.warning("STATSD_URL {statsd_url} scheme not 'statsd'")
            return

        if ":" in parsed_url.netloc:
            host, portstr = parsed_url.netloc.split(":", 1)
            port = int(portstr)  # could raise ValueError
        else:
            host = parsed_url.netloc
            port = None

        if not host:
            logger.warning("STATSD_URL {statsd_url} missing host")
            return

        realm = os.getenv("STATSD_REALM", None)
        if not realm:  # should be one of 'prod', 'staging' or developer name
            logger.warning(f"STATSD_URL {statsd_url} but STATSD_REALM not set")
            return

        prefix = f"mc.{realm}.story-indexer.{self.process_name}"
        logger.info(f"sending stats to {statsd_url} prefix {prefix}")
        self._statsd = statsd.StatsdClient(host, port, prefix)
        self._statsd._socket = SendtoSocketWrapper(self._statsd._socket)  # type: ignore[attr-defined]

    def _name(self, name: str, labels: Labels = []) -> str:
        """
        Returns a statsd suitable variable for name (may contain dots)
        and labels (in the prometheus sense), a list of (name,value) pairs.

        Hides that fact that older versions of graphite (the storage
        system) don't have any concept of labels.

        If statsd/graphite is ever replaced with a collection/storage
        system that wants to see labels separately from variable name,
        then this function's only reason for being would be to
        translate a dotted path into a form appropriate for the
        collection/storage system.

        NOTE: Sorts by label/dimension name to ensure consistent
        ordering in case multiple labels presented in differing order
        in different calls.  This MAY turn out to be a pain if you
        want to slice a chart based on one dimension (if that happens,
        add a no_sort argument to "incr" and "gauge", to pass here?
        """
        if labels:
            if TAGS:  # graphite 1.1 tags
                # https://graphite.readthedocs.io/en/latest/tags.html#tags
                # sorting may be unnecessary
                slabels = ";".join([f"{name}={val}" for name, val in sorted(labels)])
                name = f"{name};{slabels}"
            else:  # pre-1.1 graphite w/o tag support (note sorting)
                # (no arbitrary tags in netdata)
                slabels = ".".join([f"{name}_{val}" for name, val in sorted(labels)])
                name = f"{name}.{slabels}"
        return name

    def incr(self, name: str, value: int = 1, labels: Labels = []) -> None:
        """
        Increment a counter
        (something that never decreases, like an odometer)

        Please use the convention that counter names end in "s".

        NOTE!  The storage requirements for a time series are
        multiplied by the product of the number of unique values
        (cardinality) of each label, SO label values should
        be constrained to a small set!!!
        """
        if self._statsd:
            self._statsd.incr(self._name(name, labels), value)

    def gauge(self, name: str, value: float, labels: Labels = []) -> None:
        """
        Indicate value of a gauge
        (something that goes up and down, like a thermometer or speedometer)

        NOTE!  The storage requirements for a time series are
        multiplied by the product of the number of unique values
        (cardinality) of each label, SO label values should
        be constrained to a small set!!!
        """
        if self._statsd:
            self._statsd.gauge(self._name(name, labels), value)

    def timing(
        self, name: str, t: float | dt.date | dt.datetime, labels: Labels = []
    ) -> None:
        """
        Report a timing (duration) in milliseconds.  Any variable that has
        a range of values (and multiple values per period) can be
        reported as a timing.  statsd records statistics (per period)
        on the distribution of values.  If a straight histogram is
        desired, it can be added here as a tagged counter.

        NOTE!  The storage requirements for a time series are
        multiplied by the product of the number of unique values
        (cardinality) of each label, SO label values should
        be constrained to a small set!!!

        ALSO: statsd already creates NUMEROUS series for each timing!

        Now takes date and datetime too!
        """
        if self._statsd:
            ms: float
            if isinstance(t, dt.date):
                # encode date as YYYYMMDD
                ms = t.year * 10000.0 + t.month * 100.0 + t.day
            elif isinstance(t, dt.datetime):
                ms = t.timestamp() * 1000
            elif isinstance(t, int):
                ms = float(t)
            elif isinstance(t, float):
                ms = t
            else:
                return
            self._statsd.timing(self._name(name, labels), ms)

    def timer(self, name: str) -> "_TimingContext":
        """
        return "with" context for timing a block of code
        """
        return _TimingContext(self, name)

    def cleanup(self) -> None:
        """
        when overridden, call super().cleanup()
        """

    def sentry_init(self) -> None:
        """
        override to make sentry calls after sentry.init
        """
        sentry.init()

    ################ main program

    def main(self) -> None:
        ap = argparse.ArgumentParser(self.process_name, self.descr)
        self.define_options(ap)
        self.args = ap.parse_args()
        self.process_args()
        self._stats_init()
        self.sentry_init()

        with self.timer("main_loop"):  # also serves as restart count
            try:
                self.main_loop()
            finally:
                self.cleanup()

    def main_loop(self) -> None:
        """
        not necessarily a loop!
        """
        raise NotImplementedError(f"{self.__class__.__name__} must override main_loop!")


# used to be AppProtocol, based on typing.Protocol, but started getting
# "safe-super" errors from mypy for subclassed mathods. This solution from:
# https://stackoverflow.com/questions/56980077/how-to-type-python-mixin-with-superclass-calls
if TYPE_CHECKING:
    AppMixinBase = App  # base on concrete class when checking
else:
    AppMixinBase = object  # bare mixin at runtime


class _TimingContext:
    """
    a "with" context for timing a block of code
    returned by App.timing_context(name).
    """

    def __init__(self, app: App, name: str):
        self.app = app
        self.name = name
        self.t0 = -1.0

    def __enter__(self) -> None:
        assert self.t0 < 0  # make sure not active!
        self.t0 = time.monotonic()

    def __exit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        assert self.t0 > 0  # check enter'ed
        # statsd wants milliseconds:
        ms = (time.monotonic() - self.t0) * 1000
        logger.debug("%s: %g ms", self.name, ms)
        self.app.timing(self.name, ms)
        self.t0 = -1.0


class IntervalMixin(AppMixinBase):
    """
    Mixin for Apps that report stats at a fixed interval
    """

    def define_options(self, ap: argparse.ArgumentParser) -> None:
        super().define_options(ap)

        default = 60
        ap.add_argument(
            "--interval",
            type=int,
            help=f"reporting interval in seconds (default {default})",
            default=default,
        )

    def interval_sleep(self) -> None:
        assert self.args

        # sleep until top of next interval
        seconds = self.args.interval
        sleep_sec = seconds - time.time() % seconds
        logger.debug("sleep %.6g", sleep_sec)
        time.sleep(sleep_sec)


def run(klass: type[App], *args: Any, **kw: Any) -> None:
    """
    run app process
    """
    app = klass(*args, **kw)
    app.main()


if __name__ == "__main__":

    class Test(App):
        def main_loop(self) -> None:
            # allow testing of --help, --debug, --log-level
            logger.critical("critical")
            logger.error("error")
            logger.warning("warning")
            logger.info("info")
            logger.debug("debug")

    run(Test, "test", "test of app class")
