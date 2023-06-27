#!/bin/sh

. bin/func.sh

run_python indexer.workers.fetcher.enqueue_batch "$@"
