#!/bin/sh

. bin/func.sh

run_python indexer.workers.fetcher.fetch_worker --rabbitmq-url amqp://rabbitmq:5672 --batch-index 0 --fetch-date "2023-05-18" --num-batches 1 --sample-size=30
