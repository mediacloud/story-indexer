#!/bin/sh
# Run Elasticsearch playbook to do initial configuration (Index template, ILM, index creation)
# Usage: scripts/es-configure.sh [OPTIONS] [-- [EXTRA_ANSIBLE_ARGS]]
set -e
cd "$(dirname "$0")"

. ./base.sh "$@"
run_base "$@"

playbook="../playbooks/es-configure.yml"

echo "Running playbook with:"
echo "  Inventory: $inventory"
echo "  User: $user"
[ -n "$env" ] && echo "  Environment: $env"
[ -n "$extra_args" ] && echo "  Extra args: $extra_args"
echo ""

ansible-playbook "$playbook" $base_args $extra_args
