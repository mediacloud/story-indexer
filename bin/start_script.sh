#!/bin/sh

# entry point script for to configure Rabbitmq
# requires environment variable "RUN_ON_START"
ping_rabbitmq() {
  echo "Pinging RabbitMQ..."
  while ! curl -s -f http://rabbitmq:15672 > /dev/null; do
    sleep 1
  done
  echo "RabbitMQ is responsive. Proceeding with the configuration."
}

if [ -n "$RUN_ON_START" ]; then
  ping_rabbitmq
  echo "Running rabbitmq config..."
  python -m indexer.pipeline --rabbitmq-url amqp://rabbitmq:5672 configure
else
  echo "Running the common worker process..."
  exec "${APP_DOCKER}/bin/indirect.sh"
fi
