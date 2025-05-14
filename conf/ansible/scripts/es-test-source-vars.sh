#!/bin/sh
# command to run es-test-vars.yml playbook
# for testing es-test-source-envs.yml
# Usage: ./es-test-source=env.sh <environment> [ansible-playbook options]
# Example: ./es-test-vars.sh staging

# Default environment if not provided
ENVIRONMENT="${1:-local}"
shift

# Let Makefile handle venv setup (will no-op if already exists)
if ! make setup_venv >/dev/null; then
    echo "Error: Virtual environment setup failed" >&2
    exit 1
fi

exec ansible-playbook \
    -i "inventories/${ENVIRONMENT}" \
    -e "env=${ENVIRONMENT}" \
    "$@" \
    playbooks/es-test-source-vars.yml
