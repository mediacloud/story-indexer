Start of ansible scripting for ES installation

Putting this directly in the "conf" directory so that if ansible is
used for more than just ES, the directory can house that too.

### Key Files and Directories

### Makefile

"make" to install create venv with ansible installed, plus populate
role/mc.elasticsearch with a clone of the (ever so slightly) MC
modified fork of a fork of the abondoned elastic developed
ansible-elasticsearch installation role.

### es-install.sh

Script to run ansible to install ES cluster, runs playbooks/es-install.yml playbook.

To run
```
./es-install.sh --ask-vault-pass
```
The `--ask-vault-pass` argument prompts for Ansible Vault password to decrypt deployment secrets/variables in `vault.yml`

### es-uninstall.sh

Script to run the Ansible playbook for uninstalling the ES cluster.
Usage
```
./es-uninstall.sh --ask-vault-pass
```
### inventories/

Contains inventory and variable files for the Ansible playbooks

Files:

* hosts.yml - Defines the hosts and groups for the ES cluster (staging/production)

* group_vars/ - Contains group-specific variables (e.g vault.yml for encrypted secrets)

### Playbooks

Contains Ansible playbooks for managing the ES cluster

Files:

* es-install.yml - Playbook to install ES cluster, utilizes the elasticsearch role. Installs stats-d agent

* es-uninstall.yml - Playbook teardown ES cluster

### roles/elasticsearch

Contains custom Elasticsearch role used in the installation
PS: This is a cherry pocked from the abandoned `ansible-elasticsearch` rol, tailored for current use case


### tasks/install-statsd-agent.yml

Tasks file to install agent to report system stats to
statsd/graphite/grafana.  Could be used by installs for both pipeline
compute server(s) and web server(s).

### requirements.txt

Python requirements for building venv
