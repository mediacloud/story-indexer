## Docker Swarm Setup

### Prerequisites

Currently runs w/ Docker v24.0, docker-compose-plugin v21.0 under Ubuntu 22.04 LTS.

To install Docker and Docker Compose from docker.com, and create a swarm, run:

    ./docker/docker-init.sh init

To  install Docker and Docker Compose from docker.com, and join an existing swarm
(Note: we do not currently deploy containers across more than one node):

    ./docker/docker-init.sh join TOKEN ADDR:PORT

Where TOKEN and ADDR:PORT come from:

    docker swarm join-token manager

Or:

    docker swarm join-token worker


## Deploying code

### Development

With any branch OTHER than staging or prod checked out, run (as root,
or a user in the docker group):

    venv/bin/python docker/deploy.py deploy

`deploy.py` expects the branch to be clean (checked in)
and pushed to `origin`.  It will tag the git repository and the
Docker image with a tag like: `YYYY-MM-DD-HH-MM-SS-HOSTNAME-BRANCH`

Your stack will be named <USERNAME>-indexer.

#### logs

To check the health and logs of specific service within the Swarm

    docker service logs <STACK>_process_name

Log files from all containers are collected by the syslog-sink
container in (container) /app/data/logs/messages.log (log files
rotated hourly and retained for 14 days).

The logs directory can be found outside docker as
/var/lib/docker/volumes/<STACK>_worker_data/_data/logs

#### viewing/managing services

To list running services and container replica counts:

    docker service ls

or

    docker service ls --filter name=<STACK>_

To check that containers are not failing and being restarted, after a
few minutes check `docker ps` output; `docker ps -a` will show
`Exited` for containers that have terminated.

A development deploy will fetch 5000 articles once
(no restart at midnight GMT)

To scale or stop a single service:

    docker service scale SERVICE=NUMBER

To remove your stack:

    docker stack rm <STACK>

Stack volumes will be created in /var/lib/docker/volumes and will include:
    <STACK>_elasticsearch_data_01
    <STACK>_rabbitmq_data
    <STACK>_worker_data

To reset any volume:

    docker volume rm <VOLUMENAME>

A composite log file (rotated hourly) can be found in:
/var/lib/docker/volumes/<STACK>_worker_data/_data/logs/messages.log
and can be followed with "tail -F"

### Staging

Before deploying to production, the candidate code should be tested in staging:

To merge main to staging:

    git checkout staging; git merge main; git push upstream

Where `upstream` is `git@github.com:mediacloud/story-indexer.git`.

To avoid losing fixes, no changes should be checked in directly to the
`staging` branch: all changes should be merged to main first, and main
should be merged into staging.

And to deploy:

    venv/bin/python deploy.py deploy

The staging stack will be named `staging-indexer`.

A staging deploy will fetch 50000 articles once
(no restart at midnight GMT).

The staging stack can be removed after the batch has been completed.

Statistics from the staging stack are available in grafana.

### Production

Deployment to production should only be done after code has run cleanly
in staging.

As in staging, no changes should be made directly to the prod branch.

> *And the number of the counting shall be three.<br>
> Four shalt thou not count,<br>
> neither count thou two,<br>
> excepting that thou then proceed to three.<br>
> Five is right out.*

To merge run:

    git checkout prod; git merge staging; git push upstream

And to deploy run:

    venv/bin/python deploy.py deploy

You will be asked to confirm twice.

Since the fetcher starts new batches for yesterday after midnight GMT,
and runs for many hours, code should be deployed to the production
stack named `indexer` shortly after midnight GMT.

### deploy.py options

```
~/story-indexer$ venv/bin/python docker/deploy.py -h
usage: deploy [-h] [-d] [--ignore-no-changes]
              [-F {archive,csv,historical,queue-fetcher}] [-n] [-T {prod,staging}]
              {deploy,version} ...

positional arguments:
  {deploy,version}      command
    deploy              Deploy code to docker stack
    version             Display deployment package version

options:
  -h, --help            show this help message and exit
  -d, --debug           debug deployment code
  --ignore-no-changes   continue dry-run if no code or config changes
  -F {archive,csv,historical,queue-fetcher}, --flavor {archive,csv,historical,queue-fetcher}
                        instance flavor (default queue-fetcher)
  -n, --no-action       dry run: take no actions
  -T {prod,staging}, --test {prod,staging}
                        test deployment code (impl. --dry-run)


~/story-indexer$ venv/bin/python docker/deploy.py deploy -h
usage: deploy deploy [-h] [-u] [-b] [-H HIST_FILE_PREFIX] [-I INPUT_FILES]
                     [-O QUEUER_OPTS] [-Y HIST_YEAR] [-z NO_IMPORT]

options:
  -h, --help            show this help message and exit
  -u, --unpushed        allow deployment of unpushed dev repo
  -b, --build-only      build docker image then quit
  -H HIST_FILE_PREFIX, --hist-file-prefix HIST_FILE_PREFIX
                        must start with /
  -I INPUT_FILES, --input-files INPUT_FILES
                        override default input files
  -O QUEUER_OPTS, --queuer-opts QUEUER_OPTS
                        override queuer sampling options
  -Y HIST_YEAR, --hist-year HIST_YEAR
                        select year for 'historical' pipeline
  -z, --no-import
                        do not run ES importer in pipeline
```

The `-I` and `-O` options apply to "queuer" programs,
both options can be passed multiple command line
arguments (by quoting the option argument).

`-I` overrides only the default input file(s), but leaves any sampling
options (for development and staging) in place.

`-O` overrides only the sampling options (`-O ' '` can be used to turn off sampling).

## docker-compose.yml.j2 template

docker-compose.yml is generated each time from the Jinja2 template
docker-compose.yml.j2

Read the comments at the top of the template before opening a PR
with changes.

In particular:

Keep complexity out of the template

(Yes, Jinja2 is probably Turing complete, but try
to use it to just substitute values, and "ifdef"
parts in and out.)

Keep policy decisions in deploy.py, and supply
values and feature-based booleans to the template

## Testing changes to deploy.py and docker-compose.yaml.j2

The following test should be run before opening a PR with changes to
deploy.sh and docker-compose.yaml.j2 to ensure all template variables
are supplied in all environments:

```
    venv/bin/python deploy.py -n deploy
    venv/bin/python deploy.py -T staging deploy
    venv/bin/python deploy.py -T prod deploy
```

_PLB: (maybe make this part of the pre-commit checks? how??)_

## Historical (S3 CSV & HTML) story processing

An independent stack for processing historical data can be launched
with `deploy.py --flavor historical deploy`, and the stack will be
prefixed with `hist-`.

## Archive (WARC file) story processing

An independent stack for processing WARC files can be launched with
`deploy.py --flavor archive deploy`, and the stack will be prefixed
with `arch-`.
