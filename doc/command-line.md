## Common behavior

Most programs are subclasses of `indexer.app.App` and take common options:

`-h, --help` to display available options.

`--debug` to display all log messages (default is INFO)

`--quiet` to display only WARNING and higher messages.

`--list-loggers` list all logger names and exit.

`--log-level LEVEL` to set log level to one of `critical fatal error warning info debug`

`--logger-level LOGGER:LEVEL` to set LOGGER (see --list-loggers) verbosity to LEVEL (see --log-level).

Programs that interact with RabbitMQ take:

`--rabbitmq-url URL`

Programs that read from RabbitMQ queues take:

`--from-quarantine` to take messages from queue `PROGRAM-quar` instead of `PROGRAM-in`

## RabbitMQ overall configuration/status:

Set `RABBITMQ_URL` in your environment or supply a `--rabbitmq-url` option:

`./bin/run-configure-pipeline.sh show` dumps RabbitMQ queues, exchanges and bindings.

`./bin/run-configure-pipeline.sh qlen` shows RabbitMQ queue lengths (msgs and bytes).

## Manipulate individual RabbitMQ queues:

Set `RABBITMQ_URL` in your environment or supply a `--rabbitmq-url` option:

`python -m indexer.scripts.qutil purge QNAME` purges a queue (discards all messages).

`python -m indexer.scripts.qutil dump_archives QNAME` dumps queue contents as WARC files.
adding `--max MSGS` dumps at most MSGS messages.

`python -m indexer.scripts.qutil load_archives QNAME FILES...` loads WARC files into queue.
