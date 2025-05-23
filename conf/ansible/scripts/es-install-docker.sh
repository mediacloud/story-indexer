#!/bin/sh
# Run Elasticsearch playbook to install using Docker and Docker Compose
# Usage: scripts/es-install-docker.sh [OPTIONS] [-- [EXTRA_ANSIBLE_ARGS]]
#
# OPTIONS:
#   -i, --inventory INVENTORY    Ansible inventory file or directory
#   -u, --user USER             SSH user for remote connections
#   -e, --env ENVIRONMENT       Target environment (dev, staging, prod)
#   -c, --cluster CLUSTER       Target cluster (native, historical, archive)
#   -h, --help                  Show this help message
#
# EXAMPLES:
#   # Deploy native cluster to staging
#   ./es-install-docker.sh -e staging -c native
#
#   # Deploy historical cluster to dev(local)
#   ./es-install-docker.sh -e local -c historical
#
#   # Deploy archive cluster with extra ansible args
#   ./es-install-docker.sh -e local -c archive -- --check --diff

set -e
cd "$(dirname "$0")"

# Parse cluster argument
cluster=""
while [ $# -gt 0 ]; do
    case $1 in
        -c|--cluster)
            cluster="$2"
            shift 2
            ;;
        -p)
            port_offset="$2"
            shift 2
            ;;
        -h|--help)
            sed -n '2,/^$/p' "$0" | sed 's/^# \?//'
            exit 0
            ;;
        --)
            shift
            break
            ;;
        *)
            break
            ;;
    esac
done

. ./base.sh "$@"
run_base "$@"

playbook="../playbooks/es-install-docker.yml"

# Build cluster args
extra_args=""
if [ -n "$cluster" ]; then
    extra_args="-e target_cluster=$cluster"
fi
# if [ -n "$port_offset" ]; then
#     if [ -n "$cluster_args" ]; then
#         cluster_args="$cluster_args -e ports_offset=$port_offset"
#     else
#         cluster_args="-e ports_offset=$port_offset"
#     fi
# fi

echo "Running playbook with:"
echo "  Inventory: $inventory"
echo "  User: $user"
[ -n "$env" ] && echo "  Environment: $env"
[ -n "$cluster" ] && echo "  Cluster: $cluster"
[ -n "$port_offset" ] && echo "  Port offset: $port_offset"
[ -n "$extra_args" ] && echo "  Extra args: $extra_args"
echo ""

ansible-playbook "$playbook" $base_args $extra_args
