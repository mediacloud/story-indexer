#!/bin/bash

#Default Script Parameters
CLUSTER_NAME="mc_elasticsearch"
ES_VERSION="8.x"
DISCOVERY_ENDPOINTS="localhost"
# DISCOVERY_ENDPOINTS="woodward.angwin:9300,ramos.angwin:9300,bradley.angwin:9300"
NETWORK_HOST=""
DATA_BASE="/home/ubuntu/data"

help()
{
    echo "This script installs Elasticsearch cluster on Ubuntu"
    echo "Parameters:"
    echo "-n elasticsearch cluster name"
    echo "-d static discovery endpoints (comma-separated host:port or host)"
    echo "-v elasticsearch version"
    echo "-w network host"
    echo "-b data base"
    echo "-h view this help content"
}

log()
{
    echo "$1"
}

if [ "${UID}" -ne 0 ];
then
    log "Script executed without root permissions"
    echo "You must be root to run this program." >&2
    exit 1
fi

#Loop through options passed
while getopts :n:d:v:h optname; do
    log "Option $optname set with value ${OPTARG}"
  case $optname in
    n) #set cluster name
      CLUSTER_NAME=${OPTARG}
      ;;
    d) #static discovery endpoints
      DISCOVERY_ENDPOINTS=${OPTARG}
      ;;
    v) #elasticsearch version number
      ES_VERSION=${OPTARG}
      ;;
    w) #set network host
      NETWORK_HOST=${OPTARG}
      ;;
    b) #set data base
      DATA_BASE=${OPTARG}
      ;;
    h) #show help
      help
      exit 2
      ;;
    \?) #unrecognized option - show help
      echo -e \\n"Option -${BOLD}$OPTARG${NORM} not allowed."
      help
      exit 2
      ;;
  esac
done

# Check Hostname
grep -q "${HOSTNAME}" /etc/hosts
if [ $? == 0 ]
then
  echo "${HOSTNAME} found in /etc/hosts"
else
  echo "${HOSTNAME} not found in /etc/hosts"
  # Append it to the hsots file if not there
  echo "127.0.0.1 ${HOSTNAME}" >> /etc/hosts
  log "hostname ${HOSTNAME} added to /etchosts"
fi

# Configure Elasticsearch Data Disk Folder and Permissions
setup_data_disk()
{
    log "Configuring disk $1/elasticsearch/data"
    log "Configuring disk $1/elasticsearch/logs"

    mkdir -p "$1/elasticsearch/data"
    mkdir -p "$1/elasticsearch/logs"

    chown -R elasticsearch:elasticsearch "$1/elasticsearch"
    chmod 755 "$1/elasticsearch"
}

# Install Elasticsearch
install_es()
{
    # Install Debian package from APT repositoy
    # Import the Elasticsearch PGP Key
    # Install apt-transport-https package before proceeding
    # Save the repository definition to  /etc/apt/sources.list.d/elastic-8.x.list

    log "Installing Elaticsearch Version - $ES_VERSION"
    sudo wget -qO - https://artifacts.elastic.co/GPG-KEY-elasticsearch | sudo gpg --dearmor -o /usr/share/keyrings/elasticsearch-keyring.gpg
    sudo apt-get install apt-transport-https
    echo "deb [signed-by=/usr/share/keyrings/elasticsearch-keyring.gpg] https://artifacts.elastic.co/packages/$ES_VERSION/apt stable main" | sudo tee /etc/apt/sources.list.d/elastic-$ES_VERSION.list
    sudo apt-get update && sudo apt-get install elasticsearch
}

start_elasticsearch_service() {
    log "Starting Elasticsearch service on $HOSTNAME"
    sudo systemctl start elasticsearch
    if systemctl is-active --quiet elasticsearch; then
        log "Elasticsearch service started successfully"
    else
        log "Failed to start Elasticsearch service"
        exit 1
    fi
    sudo systemctl daemon-reload
    sudo systemctl enable elasticsearch
}

# Primary Install Tasks
install_es

if [ $? -eq 0 ]; then
    if [ -d "$DATA_BASE" ]; then
        setup_data_disk "$DATA_BASE"
    else
        log "Error: $DATA_BASE does not exist or is not a directory."
    fi

    # Copy the existing config file from /conf/elasticsearch to /etc/elasticsearch
    cp -f /conf/elasticsearch/elasticsearch.yml /etc/elasticsearch/elasticsearch.yml

    ELASTIC_SERVICE_FILE="/usr/lib/systemd/system/elasticsearch.service"
    if [ ! -f "$ELASTIC_SERVICE_FILE" ]; then
        echo "Service file not found: $ELASTIC_SERVICE_FILE"
        exit 1
    fi

    sed -i '/^\[Service\]/a LimitMEMLOCK=infinity' "$ELASTIC_SERVICE_FILE"

    if [ $? -eq 0 ]; then
        echo "LimitMEMLOCK set to infinity in $ELASTIC_SERVICE_FILE"
        return 0
    else
        echo "Failed to set LimitMEMLOCK in $ELASTIC_SERVICE_FILE"
        return 1
    fi

    systemctl daemon-reload

    cp -f /conf/elasticsearch/mc_jvm_options /etc/elasticsearch/jvm.options.d/mc_jvm_options

else
  echo "Elasticsearch installation failed. Aborting further setup."
  exit 1

fi

start_elasticsearch_service
