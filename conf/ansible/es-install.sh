#!/bin/sh
# create venv w/ ansible:
make
venv/bin/ansible-playbook -i es-inventory.yml $* es-install.yml
