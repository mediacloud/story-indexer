"""
Base class for command line applications
"""

import argparse
import logging
import os
import socket
import sys
import time
import urllib.parse
from logging.handlers import SysLogHandler
from types import TracebackType
from typing import Any, List, Optional, Protocol, Tuple

# PyPI
import statsd  # depends on stubs/statsd.pyi

from indexer import sentry

Labels = List[Tuple[str, Any]]  # optional labels/values for a statistic report

LEVEL_DEST = "log_level"  # args entry name!
LEVELS = [level.lower() for level in logging._nameToLevel.keys()]
LOGGER_LEVEL_SEP = ":"
TAGS = False  # get from env?? graphite >= 1.1.0 tags

logger = logging.getLogger(__name__)


class AppException(RuntimeError):
    """
    App class Exceptions
    """


class AppProtocol(Protocol):
    """
    base class for App mixins that declare & access command line args,
    or want to report stats
    """

    args: Optional[argparse.Namespace]

    def define_options(self, ap: argparse.ArgumentParser) -> None: ...

    def process_args(self) -> None: ...

    def incr(self, name: str, value: int = 1, labels: Labels = []) -> None: ...

    def gauge(self, name: str, value: float, labels: Labels = []) -> None: ...

    def timing(self, name: str, ms: float, labels: Labels = []) -> None: ...

    def timer(self, name: str) -> "_TimingContext": ...


# Dicts of log formats, indexed by App.LOG_FORMAT

# see https://docs.python.org/3/library/logging.html#logrecord-attributes
# for options in log formats.

# formats for stderr
STDERR_FORMATS = {
    "normal": "%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    "thread": "%(asctime)s | %(levelname)s | %(name)s | %(threadName)s | %(message)s",
}

# look like syslog messages (except date format),
# adds levelname; does NOT include logger name, or pid:
SYSLOG_FORMATS = {
    "normal": "%(asctime)s %(hostname)s %(app)s %(levelname)s: %(message)s",
    # include thread, formatted as if syslog pid
    "thread": "%(asctime)s %(hostname)s %(app)s[%(threadName)s] %(levelname)s: %(message)s",
}


class SendtoSocketWrapper:
    """
    Wrapper for UDP sockets used in logging.handlers.SysLogHandler and
    statsd.StatsdClient, so that socket.sendto doesn't do a DNS lookup
    on EVERY call (OR use the address resolved at startup forever,
    since it might be a container, and could be replaced at any time).
    """

    def __init__(self, actual_socket: socket.socket, cache_sec: int = 60):
        """
        defaults to caching for 60 seconds, so if address changes,
        will lose at most one minute of traffic
        """
        assert actual_socket.family == socket.AF_INET
        assert actual_socket.type == socket.SOCK_DGRAM
        self.actual_socket = actual_socket
        self.last_host = ""
        self.last_addr = ""
        self.last_lookup = 0.0
        self.cache_sec = cache_sec

    def sendto(self, data: bytes, to: Tuple[str, int]) -> int:
        """
        both SysLogHandler and Statsd only call with two args
        """
        # uses VDSO (no context switch) on x86-64 systems (at least)
        # if it's a problem, only check every N calls
        now = time.monotonic()
        if now - self.last_lookup > self.cache_sec:
            self.last_addr = ""  # invalidate cache

        to_host = to[0]
        if to_host != self.last_host or not self.last_addr:
            self.last_host = to_host
            # IPv4 only, returns single addr (round robin):
            self.last_addr = socket.gethostbyname(to_host)
            self.last_lookup = now

        return self.actual_socket.sendto(data, (self.last_addr, to[1]))


class App(AppProtocol):
    """
    Base class for command line applications (ie; Worker)
    """

    LOG_FORMAT = "normal"

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

        # NOTE! Levels applied to root logger, so effect
        # both stderr handler created by basicConfig.
        # _COULD_ apply to just stderr *handler* and
        # send everything to syslog handler.
        logging.basicConfig(format=STDERR_FORMATS[self.LOG_FORMAT], level=level)

        if self.args.logger_level:
            for ll in self.args.logger_level:
                logger_name, level = ll.split(LOGGER_LEVEL_SEP, 1)
                # XXX check logger_name in logging.root.manager.loggerDict??
                # XXX check level.upper() in LEVELS?
                logging.getLogger(logger_name).setLevel(level.upper())

        syslog_host = os.environ.get("SYSLOG_HOST", None)
        syslog_port = os.environ.get("SYSLOG_PORT", None)
        if syslog_host and syslog_port:
            # NOTE!! Using unreliable UDP because TCP connection backlog
            # can cause sends to socket to block!!

            # Could use a different LOCALn facility for different programs
            # (see note in syslog-sink.py about routing via facility).
            handler = SysLogHandler(
                address=(syslog_host, int(syslog_port)),
                facility=SysLogHandler.LOG_LOCAL0,
            )
            handler.socket = SendtoSocketWrapper(handler.socket)  # type: ignore[attr-defined]

            # additional items available to format string:
            defaults = {
                "hostname": socket.gethostname(),  # without domain
                "app": self.process_name,
            }
            fmt = SYSLOG_FORMATS[self.LOG_FORMAT]

            # Might like default datefmt includes milliseconds
            # (which aren't otherwise available)
            formatter = logging.Formatter(fmt=fmt, defaults=defaults)
            handler.setFormatter(formatter)

            # add handler to root logger
            root_logger = logging.getLogger()
            root_logger.addHandler(handler)

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

    def timing(self, name: str, ms: float, labels: Labels = []) -> None:
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

        ALSO: statd already creates NUMEROUS series for each timing!
        """
        if self._statsd:
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

    ################ main program

    def main(self) -> None:
        ap = argparse.ArgumentParser(self.process_name, self.descr)
        self.define_options(ap)
        self.args = ap.parse_args()
        self.process_args()
        self._stats_init()
        sentry.init()

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


class IntervalMixin(AppProtocol):
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
