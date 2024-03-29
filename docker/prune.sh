#!/bin/bash

#Script to cleanup docker images & containers > 7 days
# We can Optionally, remove all unused volumes and networks older than 7 days
# docker volume prune -f --filter "until=720h"
# docker network prune -f --filter "until=720h"

#Cron schedule 0 0 1 * * /docker/prune.sh

#add log file, this will b e running using cron
LOG_FILE="/var/log/docker-cleanup.log"

# Set the retention period in hours (default is 7 days)
RETENTION_PERIOD_HOURS=168

if [ "$(id -u)" -ne 0 ]; then
  echo "This script must be run as root"
  exit 1
fi

log() {
    echo "$(date +'%Y-%m-%d %H:%M:%S') - $1" >> "$LOG_FILE"
}

cleanup() {
    log "Removing all stopped containers older than 7 days"
    docker container prune -f --filter "until=${RETENTION_PERIOD_HOURS}h"

    log "Removing all unused images older than 7 days"
    docker image prune -a -f --filter "until=${RETENTION_PERIOD_HOURS}h"

    log "Docker cleanup done"
}

exit 0
