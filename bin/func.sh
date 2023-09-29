# shell functions for use in bin/*.sh
# (using /bin/sh -- not necessarily bash!)

# scripts assume the current working directory is the bin directory
# parent directory.

# save invoking script name for use by "log" function
SCRIPT=$(basename $0)

# utility for shell script logging.
log() {
    # place holder: intended to invoke scripts/logger.py
    # (an analog to the logger(1) utility) which will
    # take -p PRIO and -n TAG options
    echo log: -t $SCRIPT $* 1>&2
}

run_python() {
    MODULE=$1
    shift
    log -p debug "invoking $MODULE $*"
    python3 -m$MODULE "$@"
    STATUS=$?
    log -p debug "$MODULE $* exit status $STATUS"
    return $STATUS
}
