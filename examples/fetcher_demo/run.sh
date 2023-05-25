#!/bin/sh

# run this from top level (stories-pipeline) directory,
# after creating a virtual environment, activating, and
# installing requirements.

rm -rf /tmp/fetcher_demo
mkdir /tmp/fetcher_demo

#For now, delete the data directory on each run
rm -rf data/

python -m scripts.configure -f examples/fetcher_demo/plumbing.json configure
supervisord -c examples/fetcher_demo/fetch_demo_supervisor.conf
