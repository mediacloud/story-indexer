#!/bin/sh

# run this from top level (stories-pipeline) directory,
# after creating a virtual environment, activating, and
# installing requirements.

rm -rf /tmp/simple
mkdir /tmp/simple
python -m scripts.configure -f examples/simple/plumbing.json configure
supervisord -c examples/simple/supervisord.conf
