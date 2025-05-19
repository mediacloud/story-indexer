#!/bin/sh
# Run playbook to Install Elasticsearch on Linux (Debian/Ubuntu)
# Usage: scripts/es-install.sh [OPTIONS] [-- [EXTRA_ANSIBLE_ARGS]]
set -e
cd "$(dirname "$0")"

. ./base.sh "$@"
run_base "$@"

playbook="../playbooks/es-install.yml"

echo "Running playbook with:"
echo "  Inventory: $inventory"
echo "  User: $user"
[ -n "$env" ] && echo "  Environment: $env"
[ -n "$extra_args" ] && echo "  Extra args: $extra_args"
echo ""

ansible-playbook "$playbook" $base_args $extra_args
