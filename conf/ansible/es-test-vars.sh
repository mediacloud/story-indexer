#!/bin/sh

# command to run es-test-vars.yml playbook
# for testing es-inventory.yml and es-vars.yml

make
venv/bin/ansible-playbook \
    --connection local \
    -i es-inventory.yml \
    -e interpreter_python=/usr/bin/python3 \
    es-test-vars.yml
