#!/bin/sh

# run this from top level (stories-pipeline) directory,
# after creating a virtual environment, activating, and
# installing requirements.

rm -rf /tmp/consumer_worker
mkdir /tmp/consumer_worker
python -m scripts.configure -f plumbing.json configure
supervisord -c supervisord.conf
