#!/bin/sh
# Run Elasticsearch playbook to install using Docker and Docker Compose
# Usage: scripts/es-install-docker.sh [OPTIONS] [-- [EXTRA_ANSIBLE_ARGS]]
set -e
cd "$(dirname "$0")"

. ./base.sh "$@"
run_base "$@"

playbook="../playbooks/es-install-docker.yml"

echo "Running playbook with:"
echo "  Inventory: $inventory"
echo "  User: $user"
[ -n "$env" ] && echo "  Environment: $env"
[ -n "$extra_args" ] && echo "  Extra args: $extra_args"
echo ""

ansible-playbook "$playbook" $base_args $extra_args
