#!/bin/sh

. bin/func.sh

# honors LOG_DIR and SYSLOG_PORT:
run_python mc_logging.sink "$@"
