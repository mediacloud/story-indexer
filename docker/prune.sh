#!/bin/bash

#Script to cleanup docker images & containers > 30 days
# We can Optionally, remove all unused volumes and networks older than 30 days
# docker volume prune -f --filter "until=720h"
# docker network prune -f --filter "until=720h"

#Cron schedule 0 0 1 * * /docker/prune.sh

#add log file, this will b e running using cron
log_file="/var/log/docker-cleanup.log"

# Set the retention period in hours (default is 30 days)
retention_period_in_hours=720
retention_period_in_days=$(( $retention_period_in_hours / 24 ))

if [ "$EUID" -ne 0 ]; then
  echo "This script must be run as root"
  exit 1
fi

log() {
    echo "$(date +'%Y-%m-%d %H:%M:%S') - $1" >> "$log_file"
}

cleanup() {
    log "Removing all stopped containers older than $retention_period_in_days days"
    docker container prune -f --filter "until=${retention_period_in_hours}h"

    log "Removing all unused images older than $retention_period_in_days days"
    docker image prune -a -f --filter "until=${retention_period_in_hours}h"

    log "Docker cleanup done"
}

exit 0

