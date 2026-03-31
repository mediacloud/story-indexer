#!/bin/sh

. bin/func.sh

if [ -d indexer/workers/pipeview/versions ]; then
    alembic --config indexer/workers/pipeview/alembic.ini upgrade head
fi

run_python indexer.workers.pipeview.collector "$@"
