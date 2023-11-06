#!/bin/bash
# Deploy story-indexer

#Script Parameters
CLUSTER_NAME="mc_elasticsearch"
ES_VERSION="8.9.0"
DISCOVERY_ENDPOINTS="woodward.angwin:9300,ramos.angwin:9300,bradley.angwin:9300"
NETWORK_HOST=""
DATA_BASE="/srv/data"

help()
{
    #TODO: Add help text here
    echo "This script installs Elasticsearch cluster on Ubuntu"
    echo "Parameters:"
    echo "-n elasticsearch cluster name"
    echo "-d static discovery endpoints 10.0.0.1-3"
    echo "-v elasticsearch version"
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
    exit 3
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

set_network_host() {
  # Use ifconfig and grep to extract the IP address in the 10.x.x.x range
  NETWORK_HOST=$(ifconfig | grep -Eo 'inet (addr:)?(10\.[0-9]*\.[0-9]*\.[0-9]*)' | grep -Eo '(10\.[0-9]*\.[0-9]*\.[0-9]*)')

  if [ -z "$NETWORK_HOST" ]; then
    echo "Network interface with an IP address in the range 10.x.x.x not found."
  else
    echo "Network host: $NETWORK_HOST"
  fi
}

# Primary Install Tasks
install_es

if [ -d "$DATA_BASE" ]; then
    setup_data_disk "$DATA_BASE"
else
    log "Error: $DATA_BASE does not exist or is not a directory."
fi

if [[ -n "$DISCOVERY_ENDPOINTS" ]]; then
  HOSTS_CONFIG=("[\"${DISCOVERY_ENDPOINTS//,/\",\"}\"]")
  echo "discovery.seed_hosts: ${HOSTS_CONFIG}"
else
  echo "DISCOVERY_ENDPOINTS is empty. Please provide discovery endpoints."
fi

DATAPATH=$DATA_BASE/elasticsearch/data
LOGPATH=$DATA_BASE/elasticsearch/logs

# Configure Elasticsearch settings
#---------------------------
#Backup the current Elasticsearch configuration file
mv /etc/elasticsearch/elasticsearch.yml /etc/elasticsearch/elasticsearch.bak

# Set cluster and machine names - just use hostname for our node.name
echo "cluster.name: $CLUSTER_NAME" >> /etc/elasticsearch/elasticsearch.yml
echo "node.name: $HOSTNAME" >> /etc/elasticsearch/elasticsearch.yml
echo "path.data: $DATAPATH" >> /etc/elasticsearch/elasticsearch.yml
echo "path.logs: $LOGPATH" >> /etc/elasticsearch/elasticsearch.yml
echo "network.host: $NETWORK_HOST" >> /etc/elasticsearch/elasticsearch.yml
echo "discovery.seed_hosts: $HOSTS_CONFIG" >> /etc/elasticsearch/elasticsearch.yml

# Disable xpack.security features
# We're mostly accessing our ES instance withing a local network
echo "xpack.security.enabled: false" >> /etc/elasticsearch/elasticsearch.yml
echo "xpack.security.enrollment.enabled: false" >> /etc/elasticsearch/elasticsearch.yml
echo "xpack.security.http.ssl.enabled: false" >> /etc/elasticsearch/elasticsearch.yml
echo "xpack.security.transport.ssl.enabled: false" >> /etc/elasticsearch/elasticsearch.yml
# PS:Default transport and http ports are 9300 and 9200
#echo "http.port: 9200" >> /etc/elasticsearch/elasticsearch.yml
#transport.port: 9300 >> /etc/elasticsearch/elasticsearch.yml

#Lock memory on startup
echo "bootstrap.memory_lock: true" >> /etc/elasticsearch/elasticsearch.yml

# Update HEAP Size in this configuration or in upstart servic
CUSTOM_JVM_OPTIONS_FILE="/etc/elasticsearch/jvm.options.d/mc_jvm.options"

mkdir -p "$(dirname "$CUSTOM_JVM_OPTIONS_FILE")"

# Set the minimum and maximum heap size settings (Xms and Xmx) in the custom jvm.options file
cat <<EOL > "$CUSTOM_JVM_OPTIONS_FILE"
-Xms30g
-Xmx30g
EOL
echo "Custom JVM options have been set in $CUSTOM_JVM_OPTIONS_FILE"


log "Starting Elasticsearch on ${HOSTNAME}"
sudo systemctl start elasticsearch
sudo systemctl enable elasticsearch
log "complete elasticsearch setup and started"
