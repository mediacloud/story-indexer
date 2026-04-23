## Elasticsearch Setup

### Install Elasticseach from Debian Package

# Install Debian package from APT repositoy

# Import the Elasticsearch PGP Key
# Install apt-transport-https package before proceeding
# Save the repository definition to  /etc/apt/sources.list.d/elastic-8.x.list

sudo wget -qO - https://artifacts.elastic.co/GPG-KEY-elasticsearch | sudo gpg --dearmor -o /usr/share/keyrings/elasticsearch-keyring.gpg
sudo apt-get install apt-transport-https
echo "deb [signed-by=/usr/share/keyrings/elasticsearch-keyring.gpg] https://artifacts.elastic.co/packages/$ES_VERSION/apt stable main" | sudo tee /etc/apt/sources.list.d/elastic-$ES_VERSION.list
sudo apt-get update && sudo apt-get install elasticsearch


### Running Elasticsearch with systemd

Start elasticsearch

    `sudo systemctl start elasticsearch.service`

Stop  elasticsearch

    `sudo systemctl stop elasticsearch.service`

To configure Elasticsearch to start automatically when the system boots up, run the following commands:

    `sudo /bin/systemctl daemon-reload`
    `sudo /bin/systemctl enable elasticsearch.service`

### Configuring Elasticsearch

The Elasticsearch config directory `ES_PATH_CONF=/etc/elasticsearch`

Elasticsearch has three configuration files

    . elasticsearch.yml - configuring Elasticsearch
    . jvm.options -configure Elasticsearch JVM settings
    . log4j2.properties - configure Elasticsearch logging

The base configuration file is as specified in `doc/elasticsearch/elasticsearch.yml`, however there are node specific configurations such as `node.name`, `network.host`

#### Path settings

The data and log paths are configured to a disk mount `srv/data` as follows;

    ` cd /srv/data`
    ` sudo mkdir -p /srv/data/elasticsearch/data`
    ` sudo mkdir -p /srv/data/elasticsearch/logs`

Change ownership of the data and log directories to the `elasticsearch` user

    `sudo chown -R elasticsearch:elasticsearch /srv/data/elasticsearch/data`

    `sudo chown -R elasticsearch:elasticsearch /srv/data/elasticsearch/logs`

#### Heap size settings

By default, Elasticsearch automatically sets the JVM heap size based on a node’s roles and total memory.
We can override the default JVM options by adding a custom options file as follows

    `cd /etc/elasticsearch/jvm.options.d/ `
    `touch mc_jvm.options`

Within the custom `jvm.options` file set the minimum and maximum heap size settings `Xms` and `Xmx` as specified in `doc/elasticsearch/mc_jvm.options`

#### Configuring System Settings

When using the RPM or Debian packages on systems that use systemd, system limits must be specified via systemd.

The systemd service file (/usr/lib/systemd/system/elasticsearch.service) contains the limits that are applied by default.

1. Disable swapping

Enable memory lock by setting `bootsrap.memory_lock` to `true` in elasticsearch.yml

Set `LIMITMEMLOCK` to infinity in the systemd configuration


[Service]

    LimitMEMLOCK=infinity

2. File Descriptors

RPM and Debian packages already default the maximum number of file descriptors to 65535 and do not require further configuration.

3. Virtual Memory

The RPM and Debian packages will configure this setting automatically. No further configuration is required.


### Configuring S3 Repository for Snapshot/Restore

We are using Elasticsearch's S3 Repository plugin for Elasticsearch Snapshot & Restore

The bucket `mediacloud-elasticsearch-snapshots` has been created with the relevant policy to allow ES snapshot access to the S3 bucket.

The AWS secure credntials are added to Elasticsearch's keystore as follows:

    `./elasticsearch/blobstore_credentials.sh`

#### Register S3 repository

`
PUT _snapshot/my_s3_repository
{
  "type": "s3",
  "settings": {
    "bucket": "mediacloud-elasticsearch-snapshots",
  }
}
`
