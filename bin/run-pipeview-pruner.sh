#!/bin/sh

. bin/func.sh

# PIPEVIEW_DAYS environment variable set by docker-compose.yml
run_python indexer.workers.pipeview.pruner --delete "$@"
