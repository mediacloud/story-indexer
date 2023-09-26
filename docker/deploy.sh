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
    echo "  -d  enable debug output (on jinja2 invocation)"
    echo "  -h  output this help and exit"
    echo "  -n  dry-run: creates docker-compose.yml but does not invoke docker (implies -a -u)"
    echo "  -u  allow running as non-root user"
    exit 1
}
while getopts adhnu OPT; do
   case "$OPT" in
   a) INDEXER_ALLOW_DIRTY=1;; # allow default from environment!
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

BRANCH=$(git branch --show-current)
#GIT_HASH=$(git rev-parse --short HEAD)

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

ELASTIC_NUM_NODES=1

FETCHER_NUM_BATCHES=10

STATSD_REALM="$BRANCH"

# XXX run local instance in stack if developer (don't clutter tarbell disk)??
# depends on proxy running on tarbell
STATSD_URL=statsd://stats.tarbell.mediacloud.org

# Pushing to a local registry for now, while in dev loop.
# set registry differently based on BRANCH?!
WORKER_IMAGE_REGISTRY=$HOSTNAME:5000

WORKER_IMAGE_NAME=indexer-worker

# set DEPLOY_TIME, check remotes up to date
case "$BRANCH" in
prod|staging)
    if [ "x$INDEXER_ALLOW_DIRTY" != x ]; then
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
git fetch $REMOTE $BRANCH 2>/dev/null
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
    ELASTIC_NUM_NODES=3

    # rss-fetcher extracts package version and uses that for tag,
    # refusing to deploy if tag already exists.
    TAG=$DATE_TIME-$BRANCH
    MULTI_NODE_DEPLOYMENT=1
    # use the ones being created by staging,
    # and give staging new directories????
    echo "need VOLUME_DEVICE_PREFIX!!!!" 1>&2
    exit 1
    ;;
staging)
    STACK_NAME=staging-$BASE_STACK_NAME
    MULTI_NODE_DEPLOYMENT=1
    # correct for ramos 2023-09-23 staging deployment:
    # NOTE: **SHOULD** contain indexer-staging!!!
    VOLUME_DEVICE_PREFIX=/srv/data/docker/
    ;;
dev)
    STACK_NAME=${LOGIN_USER}-$BASE_STACK_NAME
    MULTI_NODE_DEPLOYMENT=
    # default volume storage location!
    VOLUME_DEVICE_PREFIX=/var/lib/docker/volumes/${STACK_NAME}_
    ;;
esac

if [ "x$MULTI_NODE_DEPLOYMENT" != x ]; then
    # saw problems with fetcher queuing?
    #ELASTIC_PLACEMENT_CONSTRAINT='node.labels.role_es == true'
    #WORKER_PLACEMENT_CONSTRAINT='node.labels.role_indexer == true'

    # TEMP: run everything on ramos:
    ELASTIC_PLACEMENT_CONSTRAINT='node.labels.node_name==ramos'
    WORKER_PLACEMENT_CONSTRAINT='node.labels.node_name==ramos'
else
    # default to placement on manager for (single node) developement
    ELASTIC_PLACEMENT_CONSTRAINT="node.role == manager"
    WORKER_PLACEMENT_CONSTRAINT="node.role == manager"
fi

if [ "x$IS_DIRTY" = x ]; then
    # use git tag for image tag.
    # in development this means old tagged images will pile up until removed
    # maybe have an option to prune old images?
    WORKER_IMAGE_TAG=$TAG
else
    # _could_ include DATE_TIME
    WORKER_IMAGE_TAG=$LOGIN_USER-dirty
fi

WORKER_IMAGE_FULL=$WORKER_IMAGE_REGISTRY/$WORKER_IMAGE_NAME:$WORKER_IMAGE_TAG

# allow multiple deploys on same swarm/cluster:
NETWORK_NAME=$STACK_NAME

ELASTIC_CLUSTER_NAME=mc_elasticsearch

# some commands require docker-compose.yml in the current working directory:
cd $SCRIPT_DIR

echo creating docker-compose.yml
(
  echo '# generated by jinja2: EDITING IS FUTILE'
  echo '# edit docker-compose.yml.j2 and run deploy.sh'
  if [ "x$DEBUG" != x ]; then
      # display jinja2 command:
      set -x
  fi

  # NOTE! COULD pass -Ddeploy_type=$DEPLOY_TYPE, but would rather
  # pass multiple variables that effect specific outcomes
  # (keep decision making in this file, and not template;
  #  don't ifdef based on platform name, but on features)

  # NOTE! All variables (lower_case) in sorted order,
  # set from shell UPPER_CASE shell variables of the same name.

  # from jinja2-cli package:
  jinja2 \
      -Delastic_cluster_name=$ELASTIC_CLUSTER_NAME \
      -Delastic_placement_constraint="$ELASTIC_PLACEMENT_CONSTRAINT" \
      -Delastic_num_nodes=$ELASTIC_NUM_NODES \
      -Dfetcher_num_batches=$FETCHER_NUM_BATCHES \
      -Dnetwork_name=$NETWORK_NAME \
      -Dstack_name=$STACK_NAME \
      -Dstatsd_realm=$STATSD_REALM \
      -Dstatsd_url=$STATSD_URL \
      -Dvolume_device_prefix=$VOLUME_DEVICE_PREFIX \
      -Dworker_placement_constraint="$WORKER_PLACEMENT_CONSTRAINT" \
      -Dworker_image_full=$WORKER_IMAGE_FULL \
      -Dworker_image_name=$WORKER_IMAGE_NAME \
      docker-compose.yml.j2
) > docker-compose.yml.new
STATUS=$?
if [ $STATUS != 0 ]; then
    echo "jinja2 error: $STATUS" 1>&2
    exit 2
fi
mv -f docker-compose.yml.new docker-compose.yml

echo "checking docker-compose.yml syntax" 1>&2
rm -f docker-compose.yml.dump
docker stack config -c docker-compose.yml > docker-compose.yml.dump
STATUS=$?
if [ $STATUS != 0 ]; then
    # fails w/ older versions
    echo "docker stack config status: $STATUS" 1>&2
    exit 3
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

echo ''
echo -n "Deploy branch $BRANCH as stack $STACK_NAME? [no] "
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

if [ "x$IS_DIRTY" != x ]; then
    echo adding local git tag $TAG
    git tag $TAG
    # XXX check status?

    # push tag to upstream repos
    echo pushing git tag to $REMOTE
    git push $REMOTE $TAG
    # XXX check status?
fi

echo compose build:
docker compose build
STATUS=$?
if [ $STATUS != 0 ]; then
    echo docker compose build failed: $STATUS 1>&2
    exit 1
fi

echo docker compose push:
docker compose push
STATUS=$?
if [ $STATUS != 0 ]; then
    echo docker compose build failed: $STATUS 1>&2
    exit 1
fi

echo docker stack deploy:
docker stack deploy -c docker-compose.yml $STACK_NAME
STATUS=$?
if [ $STATUS != 0 ]; then
    echo docker stack deploy failed: $STATUS 1>&2
    exit 1
fi
echo deployed.

# keep (private) record of deploys:
if [ "x$IS_DIRTY" = x ]; then
    NOTE="$REMOTE $TAG"
else
    NOTE="(dirty)"
fi
echo "$DATE_TIME $HOSTNAME $STACK_NAME $NOTE" >> deploy.log
# XXX chown to LOGIN_USER?
