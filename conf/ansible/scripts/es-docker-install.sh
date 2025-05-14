#!/bin/sh
# Run Elasticsearch playbook to install using Docker and Docker Compose
# Usage: ./run-es-docker.sh [environment] [ansible-playbook options]

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
    playbooks/es-docker.yml
