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

SCRIPT=$0
SCRIPT_DIR=$(dirname $SCRIPT)

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


usage() {
    echo "Usage: $SCRIPT [options]"
    echo "options:"
    echo "  -a  allow-dirty; no dirty/push checks; no tags applied (for dev)"
    echo "  -b  build image but do not deploy"
    echo "  -B BR -- dry run for specific branch (staging or prod)"
    echo "  -d  enable debug output (for template parameters)"
    echo "  -h  output this help and exit"
    echo "  -n  dry-run: creates docker-compose.yml but does not invoke docker (implies -a -u)"
    echo "  -u  allow running as non-root user"
    exit 1
}
while getopts B:abdhnu OPT; do
   case "$OPT" in
   a) INDEXER_ALLOW_DIRTY=1;; # allow default from environment!
   b) BUILD_ONLY=1;;
   B) NO_ACTION=1; AS_USER=1; INDEXER_ALLOW_DIRTY=1; BRANCH=$OPTARG;;
   d) DEBUG=1;;
   n) NO_ACTION=1; AS_USER=1; INDEXER_ALLOW_DIRTY=1;;
   u) AS_USER=1;;	# untested: _may_ work if user in docker group
   ?) usage;;		# here on 'h' '?' or unhandled option
   esac
done

# XXX complain if anything remaining?

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
    # XXX fall back to whoami (look by uid)
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

#GIT_HASH=$(git rev-parse --short HEAD)

dirty() {
    if [ "x$INDEXER_ALLOW_DIRTY" = x ]; then
	echo "$*" 1>&2
	exit 1
    fi
    echo "ignored: $*" 1>&2
    IS_DIRTY=1
}

# news-search-api sources in separate repo, by popular opinion,
# but deploying from our docker-compose file for network access.
# but not yet/currently building it from there.
echo looking for news-search-api docker image:
if ! docker images | grep colsearch; then
    echo news-search-api docker image not found. 1>&2
    echo 'run "docker compose build" in news-search-api repo' 1>&2
fi

if ! git diff --quiet; then
    dirty 'local changes not checked in' 1>&2
fi

# defaults for template variables that might change based on BRANCH/DEPLOY_TYPE
# (in alphabetical order):

ELASTICSEARCH_CONTAINERS=1
ELASTICSEARCH_IMAGE="docker.elastic.co/elasticsearch/elasticsearch:8.8.0"
ELASTICSEARCH_REPLICAS=1
ELASTICSEARCH_SHARDS=1

FETCHER_CRONJOB_ENABLE=true
FETCHER_NUM_BATCHES=20
FETCHER_OPTIONS="--yesterday"

NEWS_SEARCH_IMAGE_NAME=colsearch
NEWS_SEARCH_IMAGE_REGISTRY=localhost:5000/
NEWS_SEARCH_IMAGE_TAG=latest	# XXX replace with version????

RABBITMQ_CONTAINERS=1

STATSD_REALM="$BRANCH"

# XXX run local instance in stack if developer (don't clutter tarbell disk)??
# depends on proxy running on tarbell
STATSD_URL=statsd://stats.tarbell.mediacloud.org

# Pushing to a local registry for now, while in dev loop.
# set registry differently based on BRANCH?!
# MUST have trailing slash unless empty
WORKER_IMAGE_REGISTRY=localhost:5000/
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
    STACK_NAME=$BASE_STACK_NAME
    ELASTICSEARCH_CONTAINERS=0
    ELASTICSEARCH_HOSTS=http://ramos.angwin:9204,http://woodward.angwin:9200,http://bradley.angwin:9204

    # rss-fetcher extracts package version and uses that for tag,
    # refusing to deploy if tag already exists.
    TAG=$DATE_TIME-$BRANCH

    MULTI_NODE_DEPLOYMENT=1
    ;;
staging)
    STACK_NAME=staging-$BASE_STACK_NAME
    ELASTICSEARCH_CONTAINERS=3
    MULTI_NODE_DEPLOYMENT=1
    # correct for ramos 2023-09-23 staging deployment:
    # NOTE: **SHOULD** contain indexer-staging!!!
    VOLUME_DEVICE_PREFIX=/srv/data/docker/
    ;;
dev)
    # fetch limited articles under development, don't run daily
    ELASTICSEARCH_CONTAINERS=1
    ELASTICSEARCH_REPLICAS=0
    FETCHER_CRONJOB_ENABLE=false
    FETCHER_OPTIONS="$FETCHER_OPTIONS --num-batches 5000"
    FETCHER_NUM_BATCHES=10
    MULTI_NODE_DEPLOYMENT=
    STACK_NAME=${LOGIN_USER}-$BASE_STACK_NAME
    # default volume storage location!
    VOLUME_DEVICE_PREFIX=/var/lib/docker/volumes/${STACK_NAME}_
    ;;
esac

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
    WORKER_IMAGE_TAG=$TAG
else
    # _could_ include DATE_TIME
    WORKER_IMAGE_TAG=$LOGIN_USER-dirty
fi

NEWS_SEARCH_IMAGE=$NEWS_SEARCH_IMAGE_REGISTRY$NEWS_SEARCH_IMAGE_NAME:$NEWS_SEARCH_IMAGE_TAG
WORKER_IMAGE_FULL=$WORKER_IMAGE_REGISTRY$WORKER_IMAGE_NAME:$WORKER_IMAGE_TAG

# allow multiple deploys on same swarm/cluster:
NETWORK_NAME=$STACK_NAME

if [ "x$ELASTICSEARCH_CONTAINERS" != x0 ]; then
    ELASTICSEARCH_CLUSTER=mc_elasticsearch
    ELASTICSEARCH_HOSTS=http://elasticsearch1:9200
    ELASTICSEARCH_NODES=elasticsearch1
    for I in $(seq 2 $ELASTICSEARCH_CONTAINERS); do
	ELASTICSEARCH_NODES="$ELASTICSEARCH_NODES,elasticsearch$I"
	ELASTICSEARCH_HOSTS="$ELASTICSEARCH_HOSTS,http://elasticsearch$I:$(expr 9200 + $I - 1)"
    done
fi

if [ "x$RABBITMQ_CONTAINERS" = x0 ]; then
    echo "RABBITMQ_CONTAINERS is zero: need RABBITMQ_HOST!!!" 1>&2
    exit 1
else
    RABBITMQ_HOST=rabbitmq
fi
RABBITMQ_URL="amqp://$RABBITMQ_HOST:5672/?connection_attempts=10&retry_delay=5"

# some commands require docker-compose.yml in the current working directory:
cd $SCRIPT_DIR

echo creating docker-compose.yml

CONFIG=config.json
COMPOSE=docker-compose.yml.new
#trap "rm -f $CONFIG" 0

# function to add a parameter to JSON CONFIG file
add() {
    NAME=$(echo $1 | tr A-Z a-z)
    eval VALUE=\$$1
    # take optional type second, so lines sortable!
    case $2 in
    bool)
	case $VALUE in
	true|false) ;;
	*) echo add: $1 bad bool: $VALUE 1>&2; exit 1;;
	esac
	;;
    int)
	if ! echo $VALUE | egrep '^(0|[1-9][0-9]*)$' >/dev/null; then
	    echo add: $1 bad int: $VALUE 1>&2; exit 1
	fi
	;;
    str|'') VALUE='"'$VALUE'"';; # XXX maybe warn if empty??
    *) echo "add: bad type for $1: $2" 1>&2; exit 1;;
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

add ELASTICSEARCH_CLUSTER
add ELASTICSEARCH_CONTAINERS int
add ELASTICSEARCH_HOSTS
add ELASTICSEARCH_IMAGE
add ELASTICSEARCH_REPLICAS int
add ELASTICSEARCH_SHARDS int
add ELASTICSEARCH_PLACEMENT_CONSTRAINT
add FETCHER_CRONJOB_ENABLE	# NOT bool!
add FETCHER_NUM_BATCHES int
add FETCHER_OPTIONS
add NETWORK_NAME
add NEWS_SEARCH_IMAGE
add RABBITMQ_CONTAINERS int
add RABBITMQ_URL
add STACK_NAME
add STATSD_REALM
add STATSD_URL
add VOLUME_DEVICE_PREFIX
add WORKER_IMAGE_FULL
add WORKER_IMAGE_NAME
add WORKER_PLACEMENT_CONSTRAINT
echo '  "__THE_END__": true' >> $CONFIG
echo '}' >> $CONFIG

echo '# generated by jinja2: EDITING IS FUTILE' > $COMPOSE
echo '# edit docker-compose.yml.j2 and run deploy.sh' >> $COMPOSE
jinja2 docker-compose.yml.j2 $CONFIG >> $COMPOSE
STATUS=$?
if [ $STATUS != 0 ]; then
    echo "jinja2 error: $STATUS" 1>&2
    exit 2
fi
mv -f docker-compose.yml.new docker-compose.yml
chmod -w docker-compose.yml

echo "checking docker-compose.yml syntax" 1>&2
rm -f docker-compose.yml.dump
docker stack config -c docker-compose.yml > docker-compose.yml.dump
STATUS=$?
if [ $STATUS != 0 ]; then
    echo "docker stack config status: $STATUS" 1>&2
    # fails w/ older versions
    if [ $STATUS = 125 ]; then
	echo 'failed due to old version of docker??'
    else
	exit 3
    fi
else
    # maybe only keep if $DEBUG set??
    echo "output (with merges expanded) in docker-compose.yml.dump" 1>&2
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
    echo -n "Deploy from branch $BRANCH as stack $STACK_NAME? [no] "
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

if [ "x$IS_DIRTY" = x ]; then
    echo adding local git tag $TAG
    run_as_login_user git tag $TAG
    # XXX check status?

    # push tag to upstream repos
    echo pushing git tag $TAG to $REMOTE
    run_as_login_user git push $REMOTE $TAG
    # XXX check status?
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

# TEMP OFF (only run if repo not localhost?)
echo docker compose push:
docker compose push
STATUS=$?
if [ $STATUS != 0 ]; then
    echo docker compose push failed: $STATUS 1>&2
    exit 1
fi

echo 'docker stack deploy (ignore "Ignoring unsupported options: build"):'
# add --prune to remove old services?
docker stack deploy -c docker-compose.yml $STACK_NAME
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
