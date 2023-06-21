"""
Command line program to log a message.

Analagous to logger(1).
"""

import argparse
import logging

from indexer.app import LEVELS, App


class Log(App):
    def define_options(self, ap: argparse.ArgumentParser) -> None:
        super().define_options(ap)

        default_prio = "info"
        ap.add_argument(
            "--priority",
            "-p",
            choices=LEVELS,
            default=default_prio,
            help=f"set log message priority (default: {default_prio})",
        )

        default_tag = "log"
        ap.add_argument(
            "--tag",
            "-t",
            default=default_tag,
            help=f"set log message tag/program (default: {default_tag})",
        )

        # positional arguments are used as log message text
        ap.add_argument("text", nargs="+")

    def main_loop(self) -> None:
        assert self.args
        logger = logging.getLogger(self.args.tag)
        # getLevelName returns number when given (upper case) name!
        level = logging.getLevelName(self.args.priority.upper())
        logger.log(level, " ".join(self.args.text))


if __name__ == "__main__":
    log = Log("log", "command line logger")
    log.main()
