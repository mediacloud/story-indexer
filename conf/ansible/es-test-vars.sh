#!/bin/sh
# command to run es-test-vars.yml playbook
# for testing es-inventory.yml and es-vars.yml
# Usage: ./es-test-vars.sh <environment> [ansible-playbook options]
# Example: ./es-test-vars.sh production

# Set default environment to "staging" if not provided
ENVIRONMENT=${1:-staging}
make setup_venv
shift
venv/bin/ansible-playbook \
    -i "inventories/${ENVIRONMENT}/hosts.yml" \
    -e "env=${ENVIRONMENT}" \
    "$@" \
    es-test-vars.yml
