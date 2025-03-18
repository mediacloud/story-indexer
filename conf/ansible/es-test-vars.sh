#!/bin/sh

# command to run es-test-vars.yml playbook
# for testing es-inventory.yml and es-vars.yml

make setup_venv
venv/bin/ansible-playbook \
    -i inventories/staging/hosts.yml \
    "$@" \
    es-test-vars.yml
