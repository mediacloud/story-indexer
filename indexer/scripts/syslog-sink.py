"""
Simple UDP syslog sink; writes to hourly log files

Implements simple legacy BSD syslog protocol
(implemented in python SysLogHandler)
but does NOT expect legacy date/time format!!

NOT an "App" (they all log to this program!)
"""

import logging
import os
import socket
from logging.handlers import SysLogHandler
from typing import Tuple

# byte values:
NUL = 0
LT = ord("<")

MAXMSG = 64 * 1024
DEFPRIO = logging.INFO
BACKUP_DAYS = 14

# map syslog priorities to Python logging levels
SYSLOG2LOGGING = {
    SysLogHandler.LOG_EMERG: logging.CRITICAL,
    SysLogHandler.LOG_ALERT: logging.CRITICAL,
    SysLogHandler.LOG_CRIT: logging.CRITICAL,
    SysLogHandler.LOG_ERR: logging.ERROR,
    SysLogHandler.LOG_WARNING: logging.WARNING,
    SysLogHandler.LOG_NOTICE: logging.WARNING,
    SysLogHandler.LOG_INFO: logging.INFO,
    SysLogHandler.LOG_DEBUG: logging.DEBUG,
}


def parse_msg(msg: bytes) -> Tuple[int, bytes]:
    if msg[-1] == NUL:
        msg = msg[:-1]

    if msg[0] != LT or b">" not in msg[1:]:
        return (DEFPRIO, msg)

    pb, msg = msg[1:].split(b">", 1)
    prio = int(pb)
    return (prio, msg)


def main() -> None:
    # XXX implement options? min-level, log-dir, file-days

    # init logging to file:

    # get log dir, create if needed
    log_dir = os.environ.get("LOG_DIR", "/tmp")
    os.makedirs(log_dir, exist_ok=True)

    fname = os.path.join(log_dir, "messages.log")

    # new log file every hour
    handler = logging.handlers.TimedRotatingFileHandler(
        fname, when="h", utc=True, backupCount=BACKUP_DAYS * 24
    )

    root_logger = logging.getLogger()
    root_logger.addHandler(handler)
    root_logger.setLevel(logging.DEBUG)  # XXX min-level

    logging.info("syslog-sink starting")

    # init socket:
    port = int(os.environ.get("SYSLOG_PORT") or "")
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.bind(("0.0.0.0", port))

    while True:
        msg, addr = s.recvfrom(MAXMSG)
        facpri, msg = parse_msg(msg)

        if not msg:
            continue  # ignore if empty message

        ipri = facpri & SysLogHandler.LOG_DEBUG  # input prio
        logpri = SYSLOG2LOGGING[ipri]  # map to Python logging prio

        # XXX _COULD_ route to different Loggers and log files
        # (each with their own log level filters)
        # by looking at facility code (ipri >> 3)

        logging.log(logpri, msg.decode("utf-8"))


if __name__ == "__main__":
    main()
