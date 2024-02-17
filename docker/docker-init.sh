#!/bin/sh

# Install docker components for a story-indexer compute node

# see https://docs.docker.com/engine/install/ubuntu/

DISTRIB=Ubuntu
CODENAME=jammy

# XXX todo:
# start registry?
# frobbery to accept local repo w/o cert??
# apply labels w/ "docker node update"???

# uncomment when debugging this script
#set -x

if [ `whoami` != root ]; then
    echo Must be run as root 1>&2
    exit 1
fi

# find LAN address
ADDR=$(host $(hostname) | awk '/has address/ { print $4 }')
if [ "x$ADDR" = x ]; then
    echo Cannot find LAN address 1>&2
    exit 1
fi

if ! grep "^DISTRIB_ID=$DISTRIB\$" /etc/lsb-release >/dev/null || ! grep "^DISTRIB_CODENAME=$CODENAME\$" /etc/lsb-release >/dev/null; then
    echo "story indexer platform is $DISTRIB $CODENAME" 1>&2
    exit 1
fi

# POSIX standard getopts builtin
usage() {
    echo "Usage: $0 ARGS" 1>&2
    echo "Where ARGS are one of" 1>&2
    echo "to create new swarm:" 1>&2
    echo "	init" 1>&2
    echo "to join an existing swarm:" 1>&2
    echo "	join TOKEN ADDR:PORT" 1>&2
    echo "		get TOKEN and ADDR:PORT by running" 1>&2
    echo "		one of 'docker swarm join-token manager'" 1>&2
    echo "		or 'docker swarm join-token worker'" 1>&2
    echo "		on a swarm manager(?) node" 1>&2
    exit 1
}

COMMAND=$1
case "$COMMAND" in
init) INIT=1;;
join)
    TOKEN=$2; ADDR_PORT=$3;
    if ! $(echo $TOKEN) | grep '^SWMTKN' >/dev/null; then
	echo "Bad swarm token $TOKEN" 1>&2
	exit 1
    fi
    if ! $(echo $ADDR_PORT) | grep '^[0-9][0-9]*\.[0-9][0-9]*\.[0-9][0-9]*\.[0-9][0-9]*:[0-9][0-9]*' >/dev/null; then
	echo "bad ADDR:PORT $ADDR_PORT" 1>&2
	exit 1
    fi
    ;;
esac
if [ "x$INIT" = x -a "x$JOIN" = x ]; then
    usage
fi

LIST_FILE=/etc/apt/sources.list.d/docker.list
if [ -f $LIST_FILE ]; then
    echo found $LIST_FILE

    COUNT=$(dpkg --list | egrep ' (docker-ce|docker-ce-cli|containerd.io|docker-buildx-plugin|docker-compose-plugin) ' | wc -l)
    if [ "$COUNT" != 5 ]; then
	echo ERROR: expected 5 packages
	exit 1
    fi
else
    TMP=/tmp/docker-init$$
    trap "rm -f $TMP" 0
    if dpkg --list | egrep '^ii..(docker-|docker\.io|podman-docker|containerd|runc)' > $TMP; then
	echo ERROR: found docker installed without $LIST_FILE:
	cat $TMP
	exit 1
    fi

    echo "adding docker.com repository and installing docker"
    apt-get update
    apt-get install -y ca-certificates curl
    install -m 0755 -d /etc/apt/keyrings
    curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
    chmod a+r /etc/apt/keyrings/docker.asc

    echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu $CODENAME stable" > $LIST_FILE
    apt-get update

    # CAN set VERSION to "=5:24.0.0-1~ubuntu.22.04~jammy" to nail version
    apt-get install -y docker-ce$VERSION docker-ce-cli$VERSION containerd.io docker-buildx-plugin docker-compose-plugin
fi

NODE_STATE=$(docker info --format '{{.Swarm.LocalNodeState}}')
if [ "x$NODE_STATE" != xinactive ]; then
    echo "Node state is $NODE_STATE EXPECTED inactive: QUITTING" 1>&2
    exit 1
fi

if [ "x$TOKEN" != x -a "x$ADDR_PORT" != x ]; then
    echo 'NOTE! NOT YET TESTED!!'
    if docker swarm join --token $TOKEN $ADDR_PORT; then
	echo swarm join succeeded
	exit 0
    else
	echo SWARM JOIN FAILED 1>&2
	exit 1
    fi
elif [ "x$INIT" != x ]; then
    if docker swarm init --advertise-addr $ADDR; then
	echo swarm init succeeded
	exit 0
    else
	echo SWARM INIT FAILED 1>&2
	exit 1
    fi
fi
echo SHOULD NOT HAPPEN
exit 1
