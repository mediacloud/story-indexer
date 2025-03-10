#!/bin/sh

# command to run es-test-vars.yml playbook
# for testing es-inventory.yml and es-vars.yml

make
venv/bin/ansible-playbook \
    -i inventories/hosts.yml \
    $* \
    es-test-vars.yml
