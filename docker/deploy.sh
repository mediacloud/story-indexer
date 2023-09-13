#!/bin/sh

# Deploy code
# Phil Budne, 9/2023
# (from rss-fetcher/dokku-scripts/push.sh 9/2022!)

# must be run as root
# deploys from currently checked out branch

# TODO:
# generate/apply docker image tag!!!!???
# use ${DOCKER_REGISTRY} in docker-compose.yml??

# used as suffix in stack name
STACK=indexer

SCRIPT_DIR=$(dirname $0)

# tmp files to clean up on exit
REMOTES=/tmp/remotes$$
trap "rm -f $REMOTES" 0

# hostname w/o any domain
HOSTNAME=$(hostname --short)

if [ $(whoami) != root ]; then
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

if ! git diff --quiet; then
    echo 'local changes not checked in' 1>&2
    # XXX display diffs, or list dirty files??
    exit 1
fi

# XXX handle command line options here!

BRANCH=$(git branch --show-current)

# For someone that works on a branch in mediacloud repo,
# "origin" is the MCREMOTE....
ORIGIN="origin"

# PUSH_TAG_TO: other remotes to push tag to
PUSH_TAG_TO="$ORIGIN"

git remote -v > $REMOTES

# TEMP! set differently based on BRANCH?!
# use ${DOCKER_REGISTRY} in docker-compose.yaml?!!
DOCKER_REGISTRY=localhost:5000/common_worker

case "$BRANCH" in
prod|staging)
    STATSD_REALM="$BRANCH"

    # check if corresponding branch in mediacloud acct up to date

    # get remote for mediacloud account
    # ONLY match ssh remote, since will want to push tag.
    MCREMOTE=$(awk '/github\.com:mediacloud\// { print $1; exit }' $REMOTES)
    if [ "x$MCREMOTE" = x ]; then
	echo could not find an ssh git remote for mediacloud org repo
	exit 1
    fi

    # check if MCREMOTE up to date.
    if git diff --quiet $BRANCH $MCREMOTE/$BRANCH --; then
	echo "$MCREMOTE $BRANCH branch up to date."
    else
	# pushing to mediacloud repo should NOT be optional
	# for production or staging!!!
	echo "$MCREMOTE $BRANCH branch not up to date. run 'git push' first!!"
	exit 1
    fi
    # push tag back to JUST github mediacloud branch
    # (might be "origin", might not)
    PUSH_TAG_TO="$MCREMOTE"
    ;;
*)
    STATSD_REALM="$LOGIN_USER"
    # check if origin (ie; user github fork) not up to date
    # XXX need "git pull" ??
    if git diff --quiet origin/$BRANCH --; then
	echo "origin/$BRANCH up to date"
    else
	# have an option to override this??
	echo "origin/$BRANCH not up to date.  push!"
	exit 1
    fi
    ;;
esac

case $BRANCH in
prod) ;;
staging) STACK=staging-$STACK;;
*) STACK=${LOGIN_USER}-$STACK;;
esac

# XXX check if on correct server for prod/staging??

# XXX display all commits not currently deployed?
echo "Last commit:"
git log -n1

# XXX display URL for DOKKU_GIT_REMOTE??
echo ''
echo -n "Deploy branch $BRANCH as $STACK? [no] "
read CONFIRM
case "$CONFIRM" in
[yY]|[yY][eE][sS]) ;;
*) echo '[cancelled]'; exit;;
esac

if [ "x$BRANCH" = xprod ]; then
    # rss-fetcher extracts package version and uses that for tag,
    # refusing to deploy if tag already exists.
    TAG=$(date -u '+%F-%H-%M-%S')-$BRANCH

    # XXX check if pushed to github/mediacloud/PROJECT prod branch??
    # (for staging too?)

    echo -n "This is production! Type YES to confirm: "
    read CONFIRM
    if [ "x$CONFIRM" != 'xYES' ]; then
       echo '[cancelled]'
       exit
    fi
else
    # XXX use staging or $USER instead of full $STACK for brevity?
    TAG=$(date -u '+%F-%H-%M-%S')-$HOSTNAME-$STACK
fi
echo ''
echo adding local tag $TAG
git tag $TAG

# NOTE: push will complain if you (developer) switch branches
# (or your branch has been perturbed upstream, ie; by a force push)
# so add script option to enable --force to push to dokku git repo?

echo "================"

# push tag to upstream repos
for REMOTE in $PUSH_TAG_TO; do
    echo pushing tag $TAG to $REMOTE
    git push $REMOTE $TAG
    echo "================"
done

# export vars used in docker-compose.yml:
export STATSD_REALM
export DOCKER_REGISTRY

# create a /tmp/docker-compose.yml.$$ temp file
# for production if needed?
DOCKER_COMPOSE_YML=$SCRIPT_DIR/docker-compose.yml

echo compose build:
if ! docker compose build; then
    echo docker compose build failed: $? 1>&2
    exit 1
fi

echo docker compose push:
if ! docker compose push; then
    echo docker compose build failed: $? 1>&2
    exit 1
fi

echo docker stack deploy:
if ! docker stack deploy -c $DOCKER_COMPOSE_YML $STACK; then
    echo docker stack deploy failed: $? 1>&2
    exit 1
fi

# keep (private) record of deploys:
echo "$(date '+%F %T') $STACK $REMOTE $TAG" >> $SCRIPT_DIR/deploy.log
