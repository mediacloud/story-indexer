#!/bin/sh

# entry point script for Docker images

# requires environment variable "RUN", and will invoke
# bin/run-$RUN.sh, passing environ variable "ARGS as command line arguments.

. bin/func.sh

# debug: display environment
env

fatal() {
    log -p error "$0: $*"
    # avoid hard looping
    sleep 10
    exit 1
}

if [ "x$RUN" = x ]; then
    fatal "RUN not set"
fi

SCRIPT=bin/run-$RUN.sh
if [ -x $SCRIPT ]; then
    $SCRIPT $ARGS
    STATUS=$?
    log -p info $SCRIPT $ARGS exit status $STATUS
    exit $STATUS
elif [ -f $SCRIPT ]; then
    fatal "$SCRIPT not executable"
fi
fatal "$SCRIPT not found"
