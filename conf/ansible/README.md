#  Ansible Deployment Script

Ansible scripts for deploying Elasticsearch. We are putting this directly in the "conf" directory so that if ansible is
used for more than just ES, the directory can house that too.


## Key Files and Directories

### Makefile

"make" to install create venv with ansible installed. 

---

### inventories/

Contains inventory and variable files for the Ansible playbooks.

Files:

* `local/` - contains inventory files and configurations specific to the local development environment.
* `staging/` - contains inventory files and configurations specific for the staging environment.
* `production/` - contains inventory files and configurations specific for the production environment.
* `group_vars/` - contains variables that apply to specific groups. Currently, contains `all.yml` that defines variables that apply to all hosts, regardless of their group

---

### playbooks/

Contains Ansible playbooks for installing and configuring the ES cluster for production/staging/local environments.

Files:

* `es-install.yml` - automates the installation and configuration of the ES cluster on hosts belonging to the elasticsearch group.
It is intended for use on Debian or Ubuntu systems and leverages a dedicated Elasticsearch role for the main setup, 
ensuring Elasticsearch is installed natively on the host rather than in a containerized environment.


* `es-install-docker.yml` - automates the installation of the ES cluster inside Docker containers on the target host(s). 
It is intended for environments where Elasticsearch should run in a containerized setup, using Docker Compose for orchestration. 
This approach is suitable for local development, staging, or any scenario where containerization is preferred over direct installation on the host OS.


* `es-configure.yml` - automates the configuration of the ES cluster, handles Elasticsearch setup tasks including 
setting up ILM policies, creating index templates, creating initial index template, keystore configuration and SLM policy configuration.


* `es-test-source-vars.yml` - can be used to is designed to test and verify the loading and sourcing of environment variables used for ES configuration and deployment. 
It is primarily intended for debugging and validation, ensuring that all necessary variables are correctly loaded.

---
### roles/elasticsearch

Contains custom Elasticsearch role used in the installation
PS: This is cherry-picked from the abandoned `ansible-elasticsearch` role, tailored for current use case

---

### Scripts

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

---
### tasks/install-statsd-agent.yml

Tasks file to install agent to report system stats to
statsd/graphite/grafana.  Could be used by installs for both pipeline
compute server(s) and web server(s).

---
### requirements.txt

Python requirements for building venv

---
## System Requirements

- Debian/Ubuntu Linux (verified with Ubuntu 20.04/22.04)
- Minimum 4GB RAM (32GB max recommended for production)
- Java runtime (we're using bundled OpenJDK)