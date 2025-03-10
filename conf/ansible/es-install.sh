#!/bin/sh
# create venv w/ ansible:
make
# run es-install.yml playbook:
venv/bin/ansible-playbook \
    -i inventories/hosts.yml \
    $* \
    es-install.yml
