"""
Base class for command line applications
"""

import argparse
import logging
import os
import sys
import urllib.parse
from typing import Optional

# PyPI
import statsd  # using local (partial) stub file

FORMAT = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
LEVEL_DEST = "log_level"  # args entry name!
LEVELS = [level.lower() for level in logging._nameToLevel.keys()]
LOGGER_LEVEL_SEP = ":"


logger = logging.getLogger(__name__)


class AppException(Exception):
    """
    App class Exceptions
    """


class App:
    """
    Base class for command line applications (ie; Worker)
    """

    def __init__(self, process_name: str, descr: str):
        self.process_name = process_name
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

        logging.basicConfig(format=FORMAT, level=level)

        if self.args.logger_level:
            for ll in self.args.logger_level:
                logger_name, level = ll.split(LOGGER_LEVEL_SEP, 1)
                # XXX check logger_name in logging.root.manager.loggerDict??
                # XXX check level.upper() in LEVELS?
                logging.getLogger(logger_name).setLevel(level.upper())
        ################ logging now enabled

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

        prefix = f"mc.{realm}.{self.process_name}"
        logger.info(f"sending stats to {statsd_url} prefix {prefix}")
        self._statsd = statsd.StatsdClient(host, port, prefix)

    # XXX counter, gauge, time_context, time_value: all take optional tags

    ################ main program

    def main(self) -> None:
        ap = argparse.ArgumentParser(self.process_name, self.descr)
        self.define_options(ap)
        self.args = ap.parse_args()
        self.process_args()
        self._stats_init()
        self.main_loop()

    def main_loop(self) -> None:
        raise AppException(f"{self.__class__.__name__} must override main_loop!")


if __name__ == "__main__":

    class Test(App):
        def main_loop(self) -> None:
            print("here")

    t = Test("test", "test of app class")
    t.main()
