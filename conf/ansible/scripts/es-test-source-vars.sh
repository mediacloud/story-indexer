#!/bin/sh
# command to run es-test-vars.yml playbook
# for testing es-test-source-envs.yml
# Usage: ./es-test-source=env.sh <environment> [ansible-playbook options]
# Example: ./es-test-vars.sh staging
set -e
cd "$(dirname "$0")"

. ./base.sh "$@"
run_base "$@"

playbook="../playbooks/es-test-source-vars.yml"

echo "Running playbook with:"
echo "  Inventory: $inventory"
echo "  User: $user"
[ -n "$env" ] && echo "  Environment: $env"
[ -n "$extra_args" ] && echo "  Extra args: $extra_args"
echo ""

ansible-playbook "$playbook" $base_args $extra_args
