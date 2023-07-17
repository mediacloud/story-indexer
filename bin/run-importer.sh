#!/bin/sh

. bin/func.sh

run_python indexer.workers.importer --rabbitmq-url amqp://rabbitmq:5672
