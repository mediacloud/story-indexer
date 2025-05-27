#  Ansible Deployment Script

Ansible scripts for deploying Elasticsearch in production. We are putting this directly in the "conf" directory so that if ansible is
used for more than just ES, the directory can house that too. 


## Key Files and Directories

### Makefile
"make" to install create venv with ansible installed. 

### inventories/
Contains inventory and variable files for the Ansible playbooks. Currently, only the production environment is supported, 
but the structure is designed to easily accommodate inventories for additional environments in the future.

Files:
* `production/` - contains inventory files and configurations specific for the production environment.


* `group_vars/` - contains variables that apply to specific groups. Currently, contains `all.yml`
that defines variables that apply to all hosts, regardless of their group.

### playbooks/
Contains Ansible playbooks for installing and configuring the ES cluster for the production environment.

Files:
* `es-install.yml` - automates the installation and configuration of the ES cluster on hosts belonging to the elasticsearch group.
It is intended for use on Debian or Ubuntu systems and leverages a dedicated Elasticsearch role for the main setup, 
ensuring Elasticsearch is installed natively on the host rather than in a containerized environment.

### roles/elasticsearch/
Contains custom Elasticsearch role used in the installation
PS: This is cherry-picked from the abandoned `ansible-elasticsearch` role, tailored for current use case.

### scripts/
Scripts to run playbooks.
The scripts require the following options:
```
  -e, --env ENV                Environment (local, staging, production)
  -i, --inventory FILE         Inventory File
  -u, --user USER              Ansible user (default: $USER)
  -a, --ansible-args ARGS      Additional arguments to pass to ansible-playbook
  -h, --help                   Show this help message
```

Install Elasticsearch Ubuntu/Debian:
```sh
scripts/es-install.sh -e production
```

### tasks/
- `tasks/install-statsd-agent.yml` - Tasks file to install agent to report system stats to
statsd/graphite/grafana.  Could be used by installations for both pipeline
compute server(s) and web server(s).

### requirements.txt
Python requirements for building venv


## System Requirements
- Debian/Ubuntu Linux (verified with Ubuntu 20.04/22.04)
- Minimum 4GB RAM (32GB max recommended for production)
- Java runtime (we're using bundled OpenJDK)