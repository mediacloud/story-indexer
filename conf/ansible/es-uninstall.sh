#!/bin/sh
# create venv w/ ansible:
make setup_venv
# run es-install.yml playbook:
ansible-playbook \
    -i inventories/hosts.yml \
    $* \
    playbooks/es-uninstall.yml
