#!/bin/sh

. bin/func.sh

run_python indexer.workers.importer --rabbitmq-url amqp://rabbitmq:5672 --elasticsearch-host http://elasticsearch:9200/ --index-name "mediacloud_search_text"
