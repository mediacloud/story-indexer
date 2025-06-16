#!/bin/sh

# Deploy story-indexer
# Phil Budne, 9/2023
# (from rss-fetcher/dokku-scripts/push.sh 9/2022!)

# Deploys from currently checked out branch: branches staging and prod are special.

# Normally requires clean repo (all changes checked in), applies a git
# tag, and pushes the tag to github.  That way, the deploy.sh script
# and jinja2 template used are covered by the tag.

# stack name (suffix if not production)
# indicates application for peaceful coexistence!!
BASE_STACK_NAME=indexer

# in addition to developer/staging/production deployment types, there
# are now pipeline types, set by -T for handling ingest of historical
# or archival data.  They run as separate stacks, with their own
# RabbitMQ servers so the data is not co-mingled, and back-fill can be
# managed separately (ie; entirely shut down) at will.

SCRIPT=$0
SCRIPT_DIR=$(dirname $SCRIPT)

# keep created files private:
umask 077

# hostname w/o any domain
HOSTNAME=$(hostname --short)
BRANCH=$(git branch --show-current)

if [ "x$(which jinja2)" = x ]; then
    VENV_BIN=$SCRIPT_DIR/../venv/bin
    if [ -x $VENV_BIN/jinja2 ]; then
	. $VENV_BIN/activate
    else
	echo FATAL: cannot find jinja2 1>&2
	exit 3
    fi
fi

if ! python3 -m mc-manage.airtable-deployment-update  --help >/dev/null; then
    echo FATAL: deployment requires an up-to-date venv with pyairtable requirements 1>&2
    exit 3
fi

# capture command line
DEPLOYMENT_OPTIONS="$*"

PIPELINE_TYPES="batch-fetcher, historical, archive, queue-fetcher, csv"
usage() {
    # NOTE! If you change something in this function, run w/ -h
    # and put updated output into README.md file!!!
    echo "Usage: $SCRIPT [options]"
    echo "options:"
    echo "  -a      allow-dirty; no dirty/push checks; no tags applied (for dev)"
    echo "  -b      build image but do not deploy"
    echo "  -B BR   dry run for specific branch BR (ie; staging or prod, for testing)"
    echo "  -d      enable debug output (for template parameters)"
    echo "  -h      output this help and exit"
    echo "  -H /HIST_FILE_PREFIX"
    echo "          prefix for historical pipeline files (must start with /)"
    echo "  -I INPUTS"
    echo "          queuer input files/options"
    echo "  -O OPTS override queuer sampling options"
    echo "  -T TYPE select pipeline type: $PIPELINE_TYPES"
    echo "  -n      dry-run: creates docker-compose.yml but does not invoke docker (implies -a -u)"
    echo "  -u      allow running as non-root user"
    echo "  -Y HIST_YEAR"
    echo "          select year for historical pipeline"
    echo "  -z      disable import!"
    exit 1
}

PIPELINE_TYPE=queue-fetcher	# default (2024-04-22)

# if you add an option here, add to usage function above!!!
while getopts B:abdhH:I:nO:T:uY:z OPT; do
   case "$OPT" in
   a) INDEXER_ALLOW_DIRTY=1;; # allow default from environment!
   b) BUILD_ONLY=1;;
   B) NO_ACTION=1; AS_USER=1; INDEXER_ALLOW_DIRTY=1; BRANCH=$OPTARG;;
   d) DEBUG=1;;
   H) HIST_FILE_PREFIX=$OPTARG;;
   I) OPT_INPUTS="$OPTARG";;
   n) NO_ACTION=1; AS_USER=1; INDEXER_ALLOW_DIRTY=1;;
   O) OPT_OPTS="$OPTARG";;
   T) PIPELINE_TYPE=$OPTARG;;
   u) AS_USER=1;;	# untested: _may_ work if user in docker group
   Y) HIST_YEAR=$OPTARG;;
   z) NO_IMPORT=1;;
   ?) usage;;		# here on 'h' '?' or unhandled option
   esac
done

# XXX complain if anything extra on command line?

# may not be needed if user is in right (docker?) group(s)?
if [ "x$AS_USER" = x -a $(whoami) != root ]; then
    echo must be run as root 1>&2
    exit 1
fi

# get logged in user (even if su(do)ing)
# (lookup utmp entry for name of tty from stdio)
# will lose if run non-interactively via ssh (no utmp entry)
LOGIN_USER=$(who am i | awk '{ print $1 }')
if [ "x$LOGIN_USER" = x ]; then
    echo could not find login user 1>&2
    exit 1
fi

run_as_login_user() {
    if [ $(whoami) = root ]; then
	su $LOGIN_USER -c "$*"
    else
	$*
    fi
}

dirty() {
    if [ "x$INDEXER_ALLOW_DIRTY" = x ]; then
	echo "$*" 1>&2
	exit 1
    fi
    echo "ignored: $*" 1>&2
    IS_DIRTY=1
}

if ! git diff --quiet; then
    dirty 'local changes not checked in' 1>&2
fi

# defaults for template variables that might change based on BRANCH/DEPLOY_TYPE
# (in alphabetical order):

ARCHIVER_REPLICAS=1		# seems to scale 1:1 with importers

# configuration for Elastic Search Containers
ELASTICSEARCH_CLUSTER=mc_elasticsearch
ELASTICSEARCH_CONFIG_DIR=./conf/elasticsearch/templates
ELASTICSEARCH_IMAGE="docker.elastic.co/elasticsearch/elasticsearch:8.12.0"
ELASTICSEARCH_PORT_BASE=9200	# native port
ELASTICSEARCH_SNAPSHOT_CRONJOB_ENABLE=false
ELASTICSEARCH_SNAPSHOT_REPO_TYPE="fs"

FETCHER_CRONJOB_ENABLE=true	# batch fetcher
FETCHER_NUM_BATCHES=20		# batch fetcher
FETCHER_OPTIONS="--yesterday"	# batch fetcher

HIST_FETCHER_REPLICAS=4		# needs tuning

IMPORTER_REPLICAS=1

PARSER_REPLICAS=4

QUEUER_CRONJOB_ENABLE=true
QUEUER_CRONJOB_MINUTES=3-59/5
QUEUER_CRONJOB_REPLICAS=1
QUEUER_INITIAL_REPLICAS=0

RABBITMQ_CONTAINERS=1		# integer to allow cluster in staging??
RABBITMQ_PORT=5672		# native port

STATSD_REALM="$BRANCH"

# XXX run local instance in stack if developer (don't clutter tarbell disk)??
# depends on proxy running on tarbell
STATSD_URL=statsd://stats.tarbell.mediacloud.org

# must be a valid hostname (no underscores!)
SYSLOG_SINK_CONTAINER=syslog-sink

# Pushing to a local registry for now, while in dev loop.
# set registry differently based on BRANCH?!
# MUST have trailing slash unless empty
WORKER_IMAGE_REGISTRY=localhost:5000/
# PLB: maybe indexer-common, now that it's used for config & stats reporting?
WORKER_IMAGE_NAME=indexer-worker

# set DEPLOY_TIME, check remotes up to date
case "$BRANCH" in
prod|staging)
    if [ "x$INDEXER_ALLOW_DIRTY" != x -a "x$NO_ACTION" = x ]; then
	echo "dirty/notag not allowed on $BRANCH branch: ignoring" 1>&2
	INDEXER_ALLOW_DIRTY=
    fi
    DEPLOY_TYPE="$BRANCH"

    # check if corresponding branch in mediacloud acct up to date

    # get remote for mediacloud account
    # ONLY match ssh remote, since will want to push tag.
    REMOTE=$(git remote -v | awk '/github\.com:mediacloud\// { print $1; exit }')
    if [ "x$REMOTE" = x ]; then
	echo could not find an ssh git remote for mediacloud org repo 1>&2
	exit 1
    fi
    ;;
*)
    DEPLOY_TYPE=dev
    STATSD_REALM=$LOGIN_USER
    REMOTE=origin
    ;;
esac

IMPORTER_ARGS=''

# if adding anything here also add to indexer.pipeline.MyPipeline.pipe_layer method,
# and to PIPELINE_TYPES (above the usage function)!

# PIPE_TYPE_PFX effects stack (and service) name, volume directories, stats realm
case "$PIPELINE_TYPE" in
batch-fetcher)
    PIPE_TYPE_PFX=''
    PIPE_TYPE_PORT_BIAS=0	# native ports
    QUEUER_TYPE=''
    ;;
historical)
    if [ "x$HIST_YEAR" = x ]; then
	echo "need HIST_YEAR" 1>&2
	exit 1
    fi
    if [ "x$HIST_FILE_PREFIX" != x ] && \
	   ! expr "$HIST_FILE_PREFIX" : ^/ >/dev/null; then
	echo "HIST_FILE_PREFIX must begin with /" 1>&2
	exit 1
    fi
    # unless archives disabled, prefix will end with:
    ARCH_SUFFIX=hist$HIST_YEAR
    #IMPORTER_ARGS=--no-output	# uncomment to disable archives
    if [ "x$DEPLOY_TYPE" = xprod ]; then
	# In Feb 2024, on bernstein (Xeon Gold 6134@3.2GHz, 32 cores, 6400 bogomips):
	# 12 fetchers, 18 parsers, 2 importers: ~100 stories/second w/ load avg 24
	# 14 fetchers, 21 parsers, 2 importers: ~125 stories/second w/ load avg 27
	# 12 fetchers, 21 parsers, 4 importers: ~100 stories/second w/ load avg 22
	# (mean fetch: ~109ms, parse: ~150ms, import: ~11ms)
	# Aug 2024, fetching ~120 stories/sec (12 fetchers, mean 100ms), incr. to 7 importers.
	# Nov 2024: calculated that 100 stories/sec eats 30Mbit/s of bandwidth
	#    dropped to 6 fetchers, to run at 50 stories/sec
	HIST_FETCHER_REPLICAS=6
	PARSER_REPLICAS=21
	IMPORTER_REPLICAS=7
	ARCHIVER_REPLICAS=2
    else
	# Aug 2024 fetching ~40 stories/sec (w/ 4 fetchers); increased to 5 parsers
	# (4 parsers wouldn't keep up even if HIST_FETCHER_REPLICAS lowered to 3)
	PARSER_REPLICAS=5
    fi
    PIPE_TYPE_PFX='hist-'	# own stack name/queues
    PIPE_TYPE_PORT_BIAS=200	# own port range (ES has 9200+9300)

    QUEUER_FILES=s3://mediacloud-database-files/$HIST_YEAR$HIST_FILE_PREFIX
    QUEUER_TYPE='hist-queuer'	# name of run- script
    ;;
archive)
    ARCHIVER_REPLICAS=0		# no archivers
    IMPORTER_ARGS=--no-output	# no archives!!!
    if [ "x$DEPLOY_TYPE" = xprod ]; then
	# In Feb 2024, on ramos (Xeon Gold 6246R@3.4GHz 6800 bogomips)
	# 8 importers could write output of one arch-queuer (>500 stories/sec):
	IMPORTER_REPLICAS=8
    fi
    PARSER_REPLICAS=0		# no parsing required!
    PIPE_TYPE_PFX='arch-'	# own stack name/queues
    PIPE_TYPE_PORT_BIAS=400	# own port range
    # maybe require command line option to select file(s)?
    #QUEUER_FILES=s3://mediacloud-indexer-archive/2023/11/12/mc-20231112015211-1-a332941ae45f.warc.gz
    QUEUER_FILES=/app/data/archiver
    QUEUER_TYPE='arch-queuer'	# name of run- script
    ;;
queue-fetcher)
    PIPE_TYPE_PFX=''
    PIPE_TYPE_PORT_BIAS=0	# native ports
    # NOTE! If/when queue-fetcher is first used, --yesterdays (--days 1) will
    # almost certainly queue the day AFTER the day processed by the most recent
    # batch fetcher (which runs at midnight, before the rss-fetcher has written
    # the day that just ended, so it always fetches the date for 48 hours ago).
    # Using --days 2 will fetch both days (the older day first).  After N days,
    # this can be switched to "--days N" so that if a day (or more) has been
    # missed due to downtime, any holes will be filled on restart.  The code
    # that remembers what has been queued is indexer.tracker.
    QUEUER_FILES='--days 2'	# check last two days
    QUEUER_TYPE='rss-queuer'	# name of run- script
    ;;
csv)
    ARCH_SUFFIX=csv
    PIPE_TYPE_PFX='csv-'	# own stack name/queues
    PIPE_TYPE_PORT_BIAS=600
    QUEUER_TYPE='csv-queuer'
    QUEUER_FILES=s3://mediacloud-database-e-files/csv_files/
    ;;
*)
    echo "Unknown pipeline type: $PIPELINE_TYPE" 1>&2
    usage
    ;;
esac

if [ "x$NO_IMPORT" != x ]; then
    IMPORTER_ARGS="$IMPORTER_ARGS --no-import"
fi

# If explicit input files supplied for "normal" (RSS/puller) pipeline,
# change stack/stats prefix from empty to "rss-" to distinguish from
# regular/prod stack.  NOTE: rss-puller only run if OPT_INPUTS is
# empty, so will always be processing rss files.
if [ "x$OPT_INPUTS" != x -a "x$PIPE_TYPE_PFX" = x ]; then
    PIPE_TYPE_PFX=rss-
    ARCH_SUFFIX=rss
fi

# prefix stack name, stats realm with pipeline type:
BASE_STACK_NAME=$PIPE_TYPE_PFX$BASE_STACK_NAME
STATSD_REALM=$PIPE_TYPE_PFX$STATSD_REALM

# check if in sync with remote
# (send stderr to /dev/null in case remote branch does not exist)
run_as_login_user git fetch $REMOTE $BRANCH 2>/dev/null
if git diff --quiet $BRANCH $REMOTE/$BRANCH -- 2>/dev/null; then
    echo "$REMOTE $BRANCH branch up to date."
else
    dirty "$REMOTE $BRANCH branch not up to date. Run 'git push' first!!"
    # note: push could herald unwelcome news if repos have diverged!
fi

DATE_TIME=$(date -u '+%F-%H-%M-%S')
TAG=$DATE_TIME-$HOSTNAME-$BRANCH
case $DEPLOY_TYPE in
prod)
    ARCHIVER_PREFIX=mc$ARCH_SUFFIX
    PORT_BIAS=0
    STACK_NAME=$BASE_STACK_NAME

    # rss-fetcher extracts package version and uses that for tag,
    # refusing to deploy if tag already exists.
    TAG=$DATE_TIME-${PIPE_TYPE_PFX}prod

    MULTI_NODE_DEPLOYMENT=

    ELASTICSEARCH_CONTAINERS=0
    # ES index settings are static, prod settings should not change
    ELASTICSEARCH_SHARD_COUNT=30
    ELASTICSEARCH_SHARD_REPLICAS=1
    ELASTICSEARCH_ILM_MAX_AGE="90d"
    ELASTICSEARCH_ILM_MAX_SHARD_SIZE="50gb"
    ELASTICSEARCH_HOSTS=http://es.newsscribe.angwin:9209
    ELASTICSEARCH_SNAPSHOT_REPO_TYPE="s3"

    # Disabled until tested in staging.
    # Questions:
    # 1. needs additional config?
    # 2. will restart each time docker-compose deploy run??
    #ELASTICSEARCH_SNAPSHOT_CRONJOB_ENABLE=true

    # for RabbitMQ and worker_data:
    VOLUME_DEVICE_PREFIX=/srv/data/docker/${PIPE_TYPE_PFX}indexer/
    SENTRY_ENVIRONMENT="production"
    ;;
staging)
    ARCHIVER_PREFIX=staging$ARCH_SUFFIX
    STACK_NAME=staging-$BASE_STACK_NAME

    PORT_BIAS=10		# ports: prod + 10

    ELASTICSEARCH_CONTAINERS=3
    ELASTICSEARCH_SHARD_COUNT=5
    ELASTICSEARCH_SHARD_REPLICAS=1
    ELASTICSEARCH_ILM_MAX_AGE="6h"
    ELASTICSEARCH_ILM_MAX_SHARD_SIZE="5gb"
    ELASTICSEARCH_SNAPSHOT_REPO_SETTINGS_LOCATION="mc_story_indexer"

    STORY_LIMIT=50000

    # don't run daily, fetch 10x more than dev:
    FETCHER_CRONJOB_ENABLE=false
    FETCHER_OPTIONS="$FETCHER_OPTIONS --sample-size=$STORY_LIMIT"
    FETCHER_NUM_BATCHES=10	# betch fetcher

    MULTI_NODE_DEPLOYMENT=
    QUEUER_CRONJOB_ENABLE=false
    QUEUER_CRONJOB_REPLICAS=0
    QUEUER_INITIAL_REPLICAS=1
    SENTRY_ENVIRONMENT="staging"
    VOLUME_DEVICE_PREFIX=/srv/data/docker/staging-${PIPE_TYPE_PFX}indexer/
    ;;
dev)
    ARCHIVER_PREFIX=$LOGIN_USER$ARCH_SUFFIX
    # pick up from environment, so multiple dev stacks can run on same h/w cluster!
    # unless developers are running multiple ES instances, bias can be incremented
    # by one for each new developer
    PORT_BIAS=${INDEXER_DEV_PORT_BIAS:-20}

    ELASTICSEARCH_CONTAINERS=1
    ELASTICSEARCH_SHARD_COUNT=2
    ELASTICSEARCH_SHARD_REPLICAS=1
    ELASTICSEARCH_ILM_MAX_AGE="15m"
    ELASTICSEARCH_ILM_MAX_SHARD_SIZE="100mb"
    ELASTICSEARCH_SNAPSHOT_REPO_SETTINGS_LOCATION="mc_story_indexer"

    STORY_LIMIT=5000

    # batch fetcher:
    # fetch limited articles under development, don't run daily:
    FETCHER_CRONJOB_ENABLE=false
    FETCHER_OPTIONS="$FETCHER_OPTIONS --sample-size=$STORY_LIMIT"
    FETCHER_NUM_BATCHES=10

    MULTI_NODE_DEPLOYMENT=
    QUEUER_CRONJOB_ENABLE=false
    QUEUER_CRONJOB_REPLICAS=0
    QUEUER_INITIAL_REPLICAS=1
    STACK_NAME=${LOGIN_USER}-$BASE_STACK_NAME
    VOLUME_DEVICE_PREFIX=
    ;;
esac


# NOTE! in-network containers see native (unmapped) ports,
# so set environment variable values BEFORE applying PORT_BIAS!!
if [ "x$RABBITMQ_CONTAINERS" = x0 ]; then
    # if production switches to using an external (non-docker)
    # RabbitMQ cluster, set RABBITMQ_VHOST (to "/${PIPELINE_TYPE}" for
    # anything but batch-fetcher) to give each pipe-type it's own
    # queue namespace (would want to clear PIPE_TYPE_BIAS for
    # RABBITMQ_PORT!), and have index.pipeline "configure" make sure
    # vhost exists.
    echo "RABBITMQ_CONTAINERS is zero: need RABBITMQ_(V)HOST!!!" 1>&2
    exit 1
else
    RABBITMQ_HOST=rabbitmq
fi
RABBITMQ_URL="amqp://$RABBITMQ_HOST:$RABBITMQ_PORT$RABBITMQ_VHOST/?connection_attempts=10&retry_delay=5"

if [ "x$ELASTICSEARCH_CONTAINERS" != x0 ]; then
    ELASTICSEARCH_HOSTS=http://elasticsearch1:$ELASTICSEARCH_PORT_BASE
    ELASTICSEARCH_NODES=elasticsearch1
    for I in $(seq 2 $ELASTICSEARCH_CONTAINERS); do
	ELASTICSEARCH_NODES="$ELASTICSEARCH_NODES,elasticsearch$I"
	ELASTICSEARCH_HOSTS="$ELASTICSEARCH_HOSTS,http://elasticsearch$I:$ELASTICSEARCH_PORT_BASE"
    done
fi

# XXX set all of this above separately for prod/staging/dev?!
if [ "x$MULTI_NODE_DEPLOYMENT" != x ]; then
    # saw problems with fetcher queuing?
    #ELASTICSEARCH_PLACEMENT_CONSTRAINT='node.labels.role_es == true'
    #WORKER_PLACEMENT_CONSTRAINT='node.labels.role_indexer == true'

    # TEMP: run everything on ramos:
    ELASTICSEARCH_PLACEMENT_CONSTRAINT='node.labels.node_name==ramos'
    WORKER_PLACEMENT_CONSTRAINT='node.labels.node_name==ramos'
else
    # default to placement on manager for (single node) developement
    ELASTICSEARCH_PLACEMENT_CONSTRAINT="node.role == manager"
    WORKER_PLACEMENT_CONSTRAINT="node.role == manager"
fi

if [ "x$IS_DIRTY" = x ]; then
    # use git tag for image tag.
    # in development this means old tagged images will pile up until removed
    WORKER_IMAGE_TAG=$(echo $TAG | sed 's/[^a-zA-Z0-9_.-]/_/g')
else
    # _could_ include DATE_TIME, but old images can be easily pruned:
    WORKER_IMAGE_TAG=$LOGIN_USER-dirty
    # for use with git hash
    DIRTY=-dirty
fi

# identification unique to this deployment
# used as an exchange name to signal pipeline config complete
# MUST start with mc-configuration- and be less than 256 bytes:
DEPLOYMENT_ID=mc-configuration-${DATE_TIME}-${STACK_NAME}

# for context comments at top of generated docker-compose.yml:
DEPLOYMENT_BRANCH=$BRANCH
DEPLOYMENT_DATE_TIME=$DATE_TIME
DEPLOYMENT_GIT_HASH=$(git rev-parse HEAD)$DIRTY
DEPLOYMENT_HOST=$HOSTNAME
DEPLOYMENT_USER=$LOGIN_USER

WORKER_IMAGE_FULL=$WORKER_IMAGE_REGISTRY$WORKER_IMAGE_NAME:$WORKER_IMAGE_TAG

# allow multiple deploys on same swarm/cluster:
NETWORK_NAME=$STACK_NAME

# calculate exported port numbers using pipeline-type and deployment-type biases:
ELASTICSEARCH_PORT_BASE_EXPORTED=$(expr $ELASTICSEARCH_PORT_BASE + $PIPE_TYPE_PORT_BIAS + $PORT_BIAS)
RABBITMQ_PORT_EXPORTED=$(expr $RABBITMQ_PORT + $PIPE_TYPE_PORT_BIAS + $PORT_BIAS)

# some commands require docker-compose.yml in the current working directory:
cd $SCRIPT_DIR

echo creating docker-compose.yml

CONFIG=config.json
COMPOSE=docker-compose.yml.new
PRIVATE_CONF_DIR=private-conf$$
# clean up on exit unless debugging
if [ "x$DEBUG" = x ]; then
    trap "rm -f $CONFIG $COMPOSE; rm -rf $PRIVATE_CONF_DIR " 0
fi

zzz() {
    echo $1 | tr 'A-Za-z' 'N-ZA-Mn-za-m'
}

case $DEPLOY_TYPE in
prod|staging)
    rm -rf $PRIVATE_CONF_DIR
    run_as_login_user mkdir $PRIVATE_CONF_DIR
    chmod go-rwx $PRIVATE_CONF_DIR
    cd $PRIVATE_CONF_DIR
    CONFIG_REPO_PREFIX=$(zzz tvg@tvguho.pbz:zrqvnpybhq)
    CONFIG_REPO_NAME=$(zzz fgbel-vaqrkre-pbasvt)
    echo cloning $CONFIG_REPO_NAME repo 1>&2
    if ! run_as_login_user "git clone $CONFIG_REPO_PREFIX/$CONFIG_REPO_NAME.git" >/dev/null 2>&1; then
	echo "FATAL: could not clone config repo" 1>&2
	exit 1
    fi
    PRIVATE_CONF_REPO=$(pwd)/$CONFIG_REPO_NAME
    PRIVATE_CONF_FILE=$PRIVATE_CONF_REPO/$DEPLOY_TYPE.sh
    cd ..
    ;;
dev)
    # NOTE! in SCRIPT_DIR!
    USER_CONF=./${LOGIN_USER}.sh
    if [ -f $USER_CONF ]; then
	# read dev.sh for defaults, then $USER_CONF for overrides
	. ./dev.sh
	PRIVATE_CONF_FILE=$USER_CONF
    else
	PRIVATE_CONF_FILE=./dev.sh
    fi
    ;;
esac


if [ ! -f $PRIVATE_CONF_FILE ]; then
    echo "FATAL: could not access $PRIVATE_CONF_FILE" 1>&2
    exit 1
fi
echo reading config from $PRIVATE_CONF_FILE
. $PRIVATE_CONF_FILE

# after reading PRIVATE_CONF_FILE!!
case $QUEUER_TYPE in
arch-queuer)
    # borrow (r/w) key from archiver config for reading archive files
    QUEUER_S3_ACCESS_KEY_ID=$ARCHIVER_S3_ACCESS_KEY_ID
    QUEUER_S3_REGION=$ARCHIVER_S3_REGION
    QUEUER_S3_SECRET_ACCESS_KEY=$ARCHIVER_S3_SECRET_ACCESS_KEY
    ;;
rss-queuer)
    if [ "x$RSS_FETCHER_URL" != x -a "x$OPT_INPUTS" = x ]; then
        # switch to rss-puller if rss-fetcher API URL supplied (and no -I)
	QUEUER_TYPE=rss-puller
	QUEUER_FILES=
	# polling too quickly will generate many small archives
	# starting once an hour, can add /30 for twice an hour, etc
	QUEUER_CRONJOB_MINUTES=17 # 17 is MIT Random Hall "most random number"
    fi
esac

# construct QUEUER_ARGS for {arch,hist,rss}-queuers
# after reading PRIVATE_CONF and checking for RSS_FETCHER_xxx params
if [ "x$QUEUER_TYPE" != x ]; then
    # command line -O overrides sampling options
    if [ "x$OPT_OPTS" != x ]; then
	QUEUER_OPTS="$OPT_OPTS"
    elif [ "x$STORY_LIMIT" != x ]; then
	# pick random sample:
	QUEUER_OPTS="--force --sample-size $STORY_LIMIT"
    fi
    if [ "x$OPT_INPUTS" != x ]; then
	# Command line -I overrides default file(s)
	# [MAY include command line options]
	QUEUER_FILES="$OPT_INPUTS"
    fi
    QUEUER_ARGS="$QUEUER_OPTS $QUEUER_FILES"
    echo QUEUER_ARGS $QUEUER_ARGS
else
    # not applicable for batch fetcher
    QUEUER_ARGS='N/A'
fi

# things that vary by stack type, from most to least interesting
echo STACK_NAME $STACK_NAME
echo STATSD_REALM $STATSD_REALM
echo ARCHIVER_PREFIX $ARCHIVER_PREFIX

# function to add a parameter to JSON CONFIG file
add() {
    VAR=$1
    NAME=$(echo $VAR | tr A-Z a-z)
    if [ X$(eval echo \${$VAR+X}) != XX ]; then
	echo $VAR not set 1>&2
	exit 1
    fi
    eval VALUE=\$$VAR
    # take optional type second, so lines sortable!
    case $2 in
    bool)
	case $VALUE in
	true|false) ;;
	*) echo "add: $VAR bad bool: '$VALUE'" 1>&2; exit 1;;
	esac
	;;
    int)
	if ! echo $VALUE | egrep '^(0|[1-9][0-9]*)$' >/dev/null; then
	    echo "add: $VAR bad int: '$VALUE'" 1>&2; exit 1
	fi
	;;
    str|'')
	if [ "x$VALUE" = x ]; then
	    echo "add: $VAR is empty" 1>&2; exit 1
	fi
	VALUE='"'$VALUE'"'
	;;
    allow-empty)
	# ONLY use for variables that, if null turn off a feature
	VALUE='"'$VALUE'"'
	;;
    *) echo "add: $VAR bad type: '$2'" 1>&2; exit 1;;
    esac
    echo '  "'$NAME'": '$VALUE, >> $CONFIG
    if [ "x$DEBUG" != x ]; then
	echo $NAME $VALUE
    fi
}

# remove, in case old file owned by root
rm -f $CONFIG
echo '{' > $CONFIG
# NOTE! COULD pass deploy_type, but would rather
# pass multiple variables that effect specific outcomes
# (keep decision making in this file, and not template;
#  don't ifdef C code based on platform name, but on features)

# keep in alphabetical order to avoid duplicates

# When adding a new variable, you almost certainly need to add an
# environment: "FOO: {{foo}}" line in docker-compose.yaml.j2!

add ARCHIVER_B2_BUCKET		   # private
add ARCHIVER_B2_REGION allow-empty # private: empty to disable
add ARCHIVER_B2_SECRET_ACCESS_KEY  # private
add ARCHIVER_B2_ACCESS_KEY_ID	   # private
add ARCHIVER_PREFIX
add ARCHIVER_REPLICAS int
add ARCHIVER_S3_BUCKET		   # private
add ARCHIVER_S3_REGION allow-empty # private: empty to disable
add ARCHIVER_S3_SECRET_ACCESS_KEY  # private
add ARCHIVER_S3_ACCESS_KEY_ID	   # private
add DEPLOYMENT_BRANCH		   # for context
add DEPLOYMENT_DATE_TIME	   # for context
add DEPLOYMENT_GIT_HASH		   # for context
add DEPLOYMENT_HOST		   # for context
add DEPLOYMENT_ID		   # for RabbitMQ sentinal
add DEPLOYMENT_OPTIONS allow-empty # for context
add DEPLOYMENT_USER		   # for context
add ELASTICSEARCH_CLUSTER
add ELASTICSEARCH_CONFIG_DIR
add ELASTICSEARCH_CONTAINERS int
add ELASTICSEARCH_HOSTS
add ELASTICSEARCH_SNAPSHOT_CRONJOB_ENABLE # NOT bool!
add ELASTICSEARCH_SNAPSHOT_REPO
add ELASTICSEARCH_SNAPSHOT_REPO_TYPE
add ELASTICSEARCH_SNAPSHOT_REPO_SETTINGS_LOCATION allow-empty
add ELASTICSEARCH_SNAPSHOT_REPO_SETTINGS_BUCKET allow-empty #private
add ELASTICSEARCH_SNAPSHOT_REPO_SETTINGS_ENDPOINT allow-empty
add ELASTICSEARCH_SHARD_COUNT int
add ELASTICSEARCH_SHARD_REPLICAS int
add ELASTICSEARCH_ILM_MAX_AGE
add ELASTICSEARCH_ILM_MAX_SHARD_SIZE
if [ "$ELASTICSEARCH_CONTAINERS" -gt 0 ]; then
    # make these conditional rather than allow-empty
    add ELASTICSEARCH_IMAGE
    add ELASTICSEARCH_PLACEMENT_CONSTRAINT
    add ELASTICSEARCH_PORT_BASE int
    add ELASTICSEARCH_PORT_BASE_EXPORTED int
    add ELASTICSEARCH_NODES
fi
add FETCHER_CRONJOB_ENABLE	# batch-fetcher: NOT bool!
add FETCHER_NUM_BATCHES int	# batch-fetcher
add FETCHER_OPTIONS		# batch-fetcher (see QUEUER_ARGS)
add HIST_FETCHER_REPLICAS int
add IMPORTER_ARGS allow-empty
add IMPORTER_REPLICAS int
add NETWORK_NAME
add PIPELINE_TYPE
add QUEUER_ARGS
add QUEUER_CRONJOB_ENABLE	# NOT bool!
add QUEUER_CRONJOB_MINUTES	# can be *, int or range with optional /minutes
add QUEUER_CRONJOB_REPLICAS int
add QUEUER_INITIAL_REPLICAS int
add QUEUER_S3_ACCESS_KEY_ID	# private
add QUEUER_S3_REGION allow-empty # private
add QUEUER_S3_SECRET_ACCESS_KEY # private
add QUEUER_TYPE allow-empty	# empty for batch-fetcher
add PARSER_REPLICAS int
add RABBITMQ_CONTAINERS int
add RABBITMQ_PORT int
add RABBITMQ_PORT_EXPORTED int
add RABBITMQ_URL
add RSS_FETCHER_PASS allow-empty # private
add RSS_FETCHER_URL allow-empty # private
add RSS_FETCHER_USER allow-empty # private
add SENTRY_DSN allow-empty	# private: empty to disable
add SENTRY_ENVIRONMENT		# private
add STACK_NAME
add STATSD_REALM
add STATSD_URL
add SYSLOG_SINK_CONTAINER
add VOLUME_DEVICE_PREFIX allow-empty
add WORKER_IMAGE_FULL
add WORKER_IMAGE_NAME
add WORKER_PLACEMENT_CONSTRAINT
echo '  "_THE_END_": null' >> $CONFIG
echo '}' >> $CONFIG

echo '# generated by jinja2: EDITING IS FUTILE' > $COMPOSE
echo '# edit docker-compose.yml.j2 and run deploy.sh' >> $COMPOSE
jinja2 --strict docker-compose.yml.j2 $CONFIG >> $COMPOSE
STATUS=$?
if [ $STATUS != 0 ]; then
    echo "jinja2 error: $STATUS" 1>&2
    exit 2
fi
mv -f docker-compose.yml.new docker-compose.yml
chmod -w docker-compose.yml

echo "checking docker-compose.yml syntax" 1>&2
# was .dump; save using TAG for reference
DUMPFILE=docker-compose.yml.save-$TAG
rm -f $DUMPFILE
docker stack config -c docker-compose.yml > $DUMPFILE
STATUS=$?
if [ $STATUS != 0 ]; then
    echo "docker stack config status: $STATUS" 1>&2
    # fails w/ older versions
    if [ $STATUS = 125 ]; then
	echo 'failed due to old version of docker stack command??'
    else
	exit 3
    fi
else
    # maybe only keep if $DEBUG set??
    echo "output (with merges expanded) in $DUMPFILE" 1>&2
fi

# XXX check if on suitable server (right swarm?) for prod/staging??

if [ "x$NO_ACTION" != x ]; then
    echo 'dry run: quitting' 1>&2
    exit 0
fi

if [ "x$IS_DIRTY" = x ]; then
    # XXX display all commits not currently deployed?
    # use docker image tag running on stack as base??
    echo "Last commit:"
    git log -n1
else
    echo "dirty repo"
fi

if [ "x$BUILD_ONLY" = x ]; then
    echo ''
    echo -n "Deploy from branch $BRANCH stack $STACK_NAME ($PIPELINE_TYPE)? [no] "
    read CONFIRM
    case "$CONFIRM" in
    [yY]|[yY][eE][sS]) ;;
    *) echo '[cancelled]'; exit;;
    esac

    if [ "x$BRANCH" = xprod ]; then
	echo -n "This is production! Type YES to confirm: "
	read CONFIRM
	if [ "x$CONFIRM" != 'xYES' ]; then
	   echo '[cancelled]'
	   exit 0
	fi
    fi
    echo ''
fi

# apply tags before deployment
# (better to tag and not deploy, than to deploy and not tag)
if [ "x$IS_DIRTY" = x ]; then
    echo adding local git tag $TAG
    if run_as_login_user git tag $TAG; then
	echo OK
    else
	echo tag failed 1>&2
	exit 1
    fi

    # push tag to upstream repos
    echo pushing git tag $TAG to $REMOTE
    if run_as_login_user git push $REMOTE $TAG; then
	echo OK
    else
	echo tag push failed 1>&2
	exit 1
    fi

    # if config elsewhere, tag it too.
    if [ -d $PRIVATE_CONF_DIR -a -d "$PRIVATE_CONF_REPO" ]; then
	echo tagging config repo
	if (cd $PRIVATE_CONF_REPO; run_as_login_user git tag $TAG) >/dev/null 2>&1; then
	    echo OK
	else
	    echo Failed to tag $CONFIG_REPO_NAME 1>&2
	    exit 1
	fi
	echo pushing config tag
	if (cd $PRIVATE_CONF_REPO; run_as_login_user git push origin $TAG) >/dev/null 2>&1; then
	    echo OK
	else
	    echo Failed to push tag to $CONFIG_REPO_NAME 1>&2
	    exit 1
	fi
    fi
fi


echo compose build:
docker compose build
STATUS=$?
if [ $STATUS != 0 ]; then
    echo docker compose build failed: $STATUS 1>&2
    exit 1
fi

if [ "x$BUILD_ONLY" != x ]; then
    echo 'build done'
    exit 0
fi

# only needed if using multi-host swarm?
echo docker compose push:
docker compose push --quiet
STATUS=$?
if [ $STATUS != 0 ]; then
    echo docker compose push failed: $STATUS 1>&2
    exit 1
fi

echo 'docker stack deploy (ignore "Ignoring unsupported options: build"):'
# added explicit --detach to silence complaints
# add --prune to remove old services?
docker stack deploy -c docker-compose.yml --detach $STACK_NAME
STATUS=$?
if [ $STATUS != 0 ]; then
    echo docker stack deploy failed: $STATUS 1>&2
    exit 1
fi

echo deployed stack $STACK

# keep (private) record of deploys:
if [ "x$IS_DIRTY" = x ]; then
    NOTE="$REMOTE $TAG"
else
    NOTE="(dirty)"
fi
echo "$DATE_TIME $HOSTNAME $STACK_NAME $NOTE" >> deploy.log
# XXX chown to LOGIN_USER?

# optionally prune old images?

#report deployment to airtable
export AIRTABLE_API_KEY
export MEAG_BASE_ID
if [ "x$AIRTABLE_API_KEY" != x ]; then
    ##Is DEPLOYMENT_HOST always right here? as far as I can tell all the stacks get thrown onto ramos regardless.
    python3 -m mc-manage.airtable-deployment-update --codebase "story-indexer" --name $STACK_NAME --env $DEPLOYMENT_TYPE --version $IMAGE_TAG --hardware $HOSTNAME
fi
