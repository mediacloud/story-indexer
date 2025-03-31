#!/bin/sh

# This script configures Elasticsearch to use cloud storage credentials for snapshot and restore operations.
# It supports both Amazon S3 and Backblaze B2 as the cloud storage options.
#
# Usage: ./script.sh -b <blobstore>
#
# Options:
#   -b <blobstore>      Specify the cloud storage type ('s3' for Amazon S3, 'b2' for Backblaze B2).
#   -h                  Display this help message and exit.
#
# Notes:
# - This script must be run as the root user.
# - Ensure Elasticsearch is running and accessible at http://localhost:9200 before executing this script.

# Example:
#   ./elasticsearch/blobstore_credentials.sh -b s3
#

ES_PATH="/usr/share/elasticsearch"
KEY_NAME_PREFIX="s3.client.default"
ENV_FILE="prod"

LOGIN_USER=$(who am i | awk '{ print $1 }')
if [ "x$LOGIN_USER" = x ]; then
    # XXX fall back to whoami (look by uid)
    echo could not find login user 1>&2
    exit 1
fi

run_as_login_user() {
	su $LOGIN_USER -c "$*"
}

help() {
    echo "Usage: $0 -b <blobstore> [-c <container_name>]"
    echo ""
    echo "Options:"
    echo "  -b <blobstore>      Specify the blobstore type (s3 or b2)"
    echo "  -h                  Show this help message"
    echo ""
}

log() {
        echo "$1"
    }

zzz() {
    echo $1 | tr 'A-Za-z' 'N-ZA-Mn-za-m'
}

while getopts "b:h" opt; do
    case $opt in
        b)
            BLOBSTORE="$OPTARG"
            ;;
        h)
            help
            exit 0
            ;;
        *)
            help
            exit 1
            ;;
    esac
done

if [ $(whoami) != "root" ]; then
    log "ERROR: This script must be run as root."
    exit 1
fi

if [ -z "$BLOBSTORE" ]; then
    help
    exit 1
fi

if [ "$BLOBSTORE" != "s3" ] && [ "$BLOBSTORE" != "b2" ]; then
    log "ERROR: Invalid blobstore type. Use 's3' or 'b2'."
    exit 1
fi

PRIVATE_CONF_DIR="es-credentials-setup"
run_as_login_user mkdir -p $PRIVATE_CONF_DIR
chmod go-rwx $PRIVATE_CONF_DIR
log "INFO: Created private configuration directory $PRIVATE_CONF_DIR"

cd $PRIVATE_CONF_DIR
CONFIG_REPO_PREFIX=$(zzz tvg@tvguho.pbz:zrqvnpybhq)
CONFIG_REPO_NAME=$(zzz fgbel-vaqrkre-pbasvt)
PRIVATE_CONF_REPO=$(pwd)/$CONFIG_REPO_NAME

log "INFO: Cloning $CONFIG_REPO_NAME repo" 1>&2
if ! run_as_login_user git clone "$CONFIG_REPO_PREFIX/$CONFIG_REPO_NAME.git" >/dev/null 2>&1; then
    log "FATAL: could not clone config repo" 1>&2
    exit 1
fi

PRIVATE_CONF_FILE="$PRIVATE_CONF_REPO/$ENV_FILE.sh"
cd ..

if [ ! -f "$PRIVATE_CONF_FILE" ]; then
    log "FATAL: could not access $PRIVATE_CONF_FILE" 1>&2
    exit 1
fi

. "$PRIVATE_CONF_FILE"

rm -rf $PRIVATE_CONF_DIR

if [ "$BLOBSTORE" = "s3" ]; then
    ACCESS_KEY=$ELASTICSEARCH_SNAPSHOT_S3_ACCESS_KEY
    SECRET_KEY=$ELASTICSEARCH_SNAPSHOT_S3_SECRET_KEY
elif [ "$BLOBSTORE" = "b2" ]; then
    ACCESS_KEY=$ELASTICSEARCH_SNAPSHOT_B2_ACCESS_KEY
    SECRET_KEY=$ELASTICSEARCH_SNAPSHOT_B2_SECRET_KEY
fi

check_elasticsearch() {
    if curl -s "http://localhost:9200" >/dev/null 2>&1; then
        return 0
    else
        return 1
    fi
}

add_credentials() {
    log "INFO: Adding credentials to the Elasticsearch keystore"
    echo "$ACCESS_KEY" | $ES_PATH/bin/elasticsearch-keystore add --stdin --force "$KEY_NAME_PREFIX.access_key"
    echo "$SECRET_KEY" | $ES_PATH/bin/elasticsearch-keystore add --stdin --force "$KEY_NAME_PREFIX.secret_key"
}

# Function to reload Elasticsearch secure settings
reload_secure_settings() {
    log "INFO: Reloading Elasticsearch secure settings"
    curl -X POST "http://localhost:9200/_nodes/reload_secure_settings" -H "Content-Type: application/json" -d '{}'
}

if check_elasticsearch; then
    add_credentials
    if [ $? -eq 0 ]; then
        reload_secure_settings
        if [ $? -eq 0 ]; then
            log "INFO: Credentials for $BLOBSTORE added and secure settings reloaded successfully."
        else
            log "ERROR: Failed to reload secure settings."
            exit 1
        fi
    else
        log "ERROR: Failed to add credentials to the keystore."
        exit 1
    fi
else
    log "ERROR: Elasticsearch is not running or reachable. Exiting."
    exit 1
fi
