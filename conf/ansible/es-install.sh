#!/bin/sh
# create venv w/ ansible:
make setup_venv
# run es-install.yml playbook:
venv/bin/ansible-playbook \
    -i inventories/production/hosts.yml \
    "$@" \
    playbooks/es-install.yml
