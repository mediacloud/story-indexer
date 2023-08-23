#!/bin/sh

. bin/func.sh

run_python indexer.workers.parser --rabbitmq-url amqp://rabbitmq:5672
