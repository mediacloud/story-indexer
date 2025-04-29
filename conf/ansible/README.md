Start of ansible scripting for ES installation

Putting this directly in the "conf" directory so that if ansible is
used for more than just ES, the directory can house that too.

## Key Files and Directories

### Makefile

"make" to install create venv with ansible installed, plus populate
role/mc.elasticsearch with a clone of the (ever so slightly) MC
modified fork of a fork of the abondoned elastic developed
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
* `roles/elasticsearch/` - Contains all installation tasks and configuration:
  * `tasks/main.yml` - Core installation tasks
  * `tasks/post_install.yml` - Post-installation configuration
  * `tasks/install-statsd-agent.yml` - Monitoring agent setup

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
- Minimum 4GB RAM (8GB recommended for production)
- Java runtime (installed automatically)

##### Usage

1. Verify variables in inventory and group_vars
2. Run the installation playbook

Example command:
```sh
ansible-playbook playbooks/es-install.yml -i inventories/production/hosts.yml
```

```sh
./es-install.sh --inventory inventories/production/hosts.yml --user deploy --ask-become-pass
```

#### Elasticsearch Configuration Playbook

This playbook handles core Elasticsearch configuration including ILM policies, index templates, and initial index creation.

##### Purpose
- Configure Index Lifecycle Management (ILM) policies
- Create index templates for consistent index settings/mappings
- Bootstrap initial indices with proper lifecycle configuration

##### Files

* `playbooks/es-configure.yml` - Main playbook for ES configuration
* `roles/elasticsearch/tasks/` - Contains task files for each configuration component:
  * `create_ilm_policy.yml` - ILM policy configuration
  * `create_index_template.yml` - Index template creation
  * `create_initial_index.yml` - Initial index bootstrapping

##### Configuration Parameters

The playbook uses these variables (typically defined in role defaults or inventory):

| Variable               | Description                                                                 | Location                  |
|------------------------|-----------------------------------------------------------------------------|---------------------------|
| `es_api_scheme`       | HTTP scheme for Elasticsearch API (http/https)                             | role defaults             |
| `es_api_host`         | Elasticsearch host                                                         | role defaults             |
| `es_api_port`         | Elasticsearch API port                                                     | role defaults             |
| ILM-related vars      | Policy names, rollover conditions, etc.                                    | role defaults             |
| Template-related vars | Index patterns, mappings, settings                                         | role defaults             |

##### Usage

1. Verify variables are properly configured in role defaults or inventory
2. Run the entire playbook or specific tasks using tags

Example commands:

```
ansible-playbook playbooks/es-configure.yml -i inventories/production/hosts.yml --tags es_ilm      # Only ILM policies
ansible-playbook playbooks/es-configure.yml -i inventories/production/hosts.yml --tags es_template # Only index templates
ansible-playbook playbooks/es-configure.yml -i inventories/production/hosts.yml --tags es_index    # Only initial index
```

```sh
./configure_es.sh --user <> --become-pass <>

./configure_es.sh --template-only --user <> --become-pass <> # Only index templates

./configure_es.sh --ilm-only --user <> --become-pass <> # Only ILM policies
```

#### Elasticsearch Reindexing Playbook

This playbook handles data re-indexing from a remote Elasticsearch cluster to a local cluster with configurable parameters.

##### Purpose

- Re-index data from a source index on a remote cluster to a destination index on the local cluster
- Supports date range filtering during re-indexing
- Configurable batch size for optimal performance

##### Files
* `es-reindex.yml` - Main playbook for re-indexing operations
* `roles/elasticsearch/tasks/elasticsearch-reindex.yml` - Contains the actual re-indexing tasks

##### Configuration Parameters
The playbook accepts the following variables (can be passed via command line or inventory):

| Variable               | Description                                                                 | Default Value       |
|------------------------|-----------------------------------------------------------------------------|---------------------|
| `source_index`         | Name of the source index on remote cluster                                  | (required)          |
| `dest_index`          | Name of the destination index on local cluster                             | (required)          |
| `reindex_date_from`   | Start date for filtering documents to re-index (format: YYYY-MM-DD)          | (required)          |
| `reindex_date_to`     | End date for filtering documents to re-index (format: YYYY-MM-DD)            | (required)          |
| `es_reindex_batch_size` | Number of documents to process in each batch                              | Defined in role     |
| `es_api_scheme`       | HTTP scheme for Elasticsearch API (http/https)                             | Defined in role     |
| `es_api_host`         | Elasticsearch host                                                         | Defined in role     |
| `es_api_port`         | Elasticsearch API port                                                     | Defined in role     |

#### Usage
1. Ensure all required variables are set (either in inventory or via command line)
2. Run the playbook using the provided wrapper script or directly with ansible-playbook

Example command:

```
ansible-playbook es-reindex.yml \
  -e "source_index=mc_search-000001" \
  -e "dest_index=mc_search" \
  -e "reindex_date_from=2024-04-01" \
  -e "reindex_date_to=2024-05-31" \
  -e "es_reindex_batch_size=5000"
```

```sh
./es-reindex.sh --source-index mc_search-000001 --dest-index mc_search \
                 --date-from 2024-04-01 --date-to 2024-05-31 \
                 --batch-size 5000
```

### roles/elasticsearch

Contains custom Elasticsearch role used in the installation
PS: This is a cherry pocked from the abandoned `ansible-elasticsearch` rol, tailored for current use case


### tasks/install-statsd-agent.yml

Tasks file to install agent to report system stats to
statsd/graphite/grafana.  Could be used by installs for both pipeline
compute server(s) and web server(s).

### requirements.txt

Python requirements for building venv
