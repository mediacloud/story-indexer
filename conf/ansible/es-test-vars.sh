#!/bin/sh
# command to run es-test-vars.yml playbook
# for testing es-inventory.yml and es-vars.yml
# Usage ./es-test-vars.sh <environment>
# e.g ./es-test-vars.sh production
ENVIRONMENT=${1:-staging}

make setup_venv

venv/bin/ansible-playbook \
    -i "inventories/${ENVIRONMENT}/hosts.yml" \
    -e "env=${ENVIRONMENT}" \
    "${@:2}" \
    es-test-vars.yml
