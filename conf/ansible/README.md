Start of ansible scripting for ES installation

Putting this directly in the "conf" directory so that if ansible is
used for more than just ES, the directory can house that too.

## Key Files and Directories

### Makefile

"make" to install create venv with ansible installed, plus populate
role/mc.elasticsearch with a clone of the (ever so slightly) MC
modified fork of a fork of the abandoned elastic developed
ansible-elasticsearch installation role.

### Inventories/

Contains inventory and variable files for the Ansible playbooks

Files:

* hosts.yml - Defines the hosts and groups for the ES cluster (staging/production)

* group_vars/ - Contains group-specific variables (e.g vault.yml for encrypted secrets)

### Playbooks

Contains Ansible playbooks for managing the ES cluster

#### Elasticsearch Installation Playbook

This playbook handles the complete installation and initial configuration of Elasticsearch clusters.

##### Purpose
- Install Elasticsearch packages and dependencies
- Configure system settings for optimal Elasticsearch performance
- Set up basic security and monitoring
- Validate cluster health post-installation

##### Files

* `playbooks/es-install.yml` - Main playbook for ES installation
* `playbooks/es-install-docker.yml` - Playbook for ES installation using Docker and Docker Compose
* `roles/elasticsearch/` - Contains all installation tasks and configuration:
  * `tasks/main.yml` - Core installation tasks
  * `tasks/install-statsd-agent.yml` - Monitoring agent setup
* `tasks/` - Contains tasks that are not necessarily for the Role
  * `tasks/source-envs.yml` - Source env variables used by story-indexer
  * `tasks/load-envs.yml` - Load all env variables

##### Configuration Parameters

The playbook uses these key variables:

| Variable               | Description                                                                 | Location                  |
|------------------------|-----------------------------------------------------------------------------|---------------------------|
| `es_version`          | Version of Elasticsearch to install                                        | inventory/group_vars      |
| `es_cluster_name`     | Name for the Elasticsearch cluster                                         | inventory/group_vars      |
| `es_heap_size`        | JVM heap size allocation                                                   | role defaults             |
| `es_api_port`         | Elasticsearch API port                                                     | role defaults             |
| `es_discovery_seeds`  | List of seed nodes for cluster discovery                                   | inventory/group_vars      |

##### System Requirements

- Debian/Ubuntu Linux (verified with Ubuntu 20.04/22.04)
- Minimum 4GB RAM (32GB max recommended for production)
- Java runtime (we're using bundled OpenJDK)


##### Scripts

Scripts to run playbooks.
The scripts require the following options
Options:
  -e, --env ENV           Environment (local, staging, production)
  -i, --inventory FILE    Inventory File
  -u, --user USER         Ansible user (default: $USER)
  -h, --help              Show this help message

Test environment variables:
```sh
scripts/es-test-source-vars.sh -e local
```

Install Elasticsearch Docker compose staging:
```sh
scripts/es-install-docker.sh -e staging
```

Install Elasticsearch Ubuntu/Debian:
```sh
scripts/es-install.sh -e production
```

Do Elasticsearch cluster configuration:
```sh
scripts/es-configure.sh -e staging
```

### roles/elasticsearch

Contains custom Elasticsearch role used in the installation
PS: This is cherry picked from the abandoned `ansible-elasticsearch` role, tailored for current use case


### tasks/install-statsd-agent.yml

Tasks file to install agent to report system stats to
statsd/graphite/grafana.  Could be used by installs for both pipeline
compute server(s) and web server(s).

### requirements.txt

Python requirements for building venv
