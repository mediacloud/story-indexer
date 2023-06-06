MC Story Pipeline Demo
======================

Requires RabbitMQ to be set up- I'm using the docker image as described here: https://www.rabbitmq.com/download.html
Also requires an environment variable RABBITMQ_URL to be set- should look something like 'amqp://guest:guest@0.0.0.0:5672/' by default.


1. Fetcher Testing -
2. From fetcher - to - plugin elastic-pipeline


Running using Docker compose

    `docker compose build && docker compose up`
