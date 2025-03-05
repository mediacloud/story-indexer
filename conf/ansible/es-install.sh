#!/bin/sh
# create venv w/ ansible:
make
# run es-install.yml playbook:
venv/bin/ansible-playbook \
    --connection elasticsearch \
    -i es-inventory.yml \
    $* \
    es-install.yml
