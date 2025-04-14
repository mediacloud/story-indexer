Start of ansible scripting for ES installation

Putting this directly in the "conf" directory so that if ansible is
used for more than just ES, the directory can house that too.

* Makefile

"make" to install create venv with ansible installed, plus populate
role/mc.elasticsearch with a clone of the (ever so slightly) MC
modified fork of a fork of the abondoned elastic developed
ansible-elasticsearch installation role.

* es-install.sh

Script to run ansible to install ES cluster, runs es-install.yml playbook.

* es-install.yml

Start of playbook to install ES cluster.  Currently only installs ES,
Java, statsd-agent, and preliminary config.

Does not start ES or attempt to establish a cluster!

* es-inventory.yml

YAML inventory file (host list) for running es-install.yml.  Could be
merged into a single inventory.yml, but currently only ES hosts AND
per-host config settings!

* es-vars.yml

Global vars for ES install.

* install-statsd-agent.yml

Tasks file to install agent to report system stats to
statsd/graphite/grafana.  Could be used by installs for both pipeline
compute server(s) and web server(s).

* requirements.txt

Python requirements for building venv
