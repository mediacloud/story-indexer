#!/bin/sh

# entry point script for to configure Rabbitmq
# requires environment variable "RUN_ON_START"
if [ -n "$RUN_ON_START" ]; then
  echo "Running rabbitmq config..."
  python -m indexer.pipeline --rabbitmq-url amqp://rabbitmq:5672 configure
else
  echo "Running the common worker process..."
  exec "${APP_DOCKER}/bin/indirect.sh"
fi
