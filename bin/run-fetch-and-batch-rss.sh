#!/bin/sh

. bin/func.sh

run_python indexer.workers.fetcher.fetch_and_batch_rss "$@"
