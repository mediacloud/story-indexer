#!/bin/sh

# could select different files based on an environment variable:
PLUMBING_JSON=plumbing.json
SUPERVISOR_CONF=supervisord.conf

# create queues:
python -m scripts.configure -f $PLUMBING_JSON configure

# create log directory (if needed) for log files (in non-volatile "storage")
# XXX get from dokku config:set!!
# used in supervisord.conf!!
export LOGDIR=/app/storage/logs
test -d $LOGDIR || mkdir -p $LOGDIR

# run processes:
supervisord -c $SUPERVISORD_CONF
