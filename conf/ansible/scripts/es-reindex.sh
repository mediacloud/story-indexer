#!/bin/sh
# Run Elasticsearch playbook to reindex data (one-time only)
# Usage: scripts/es-reindex.sh [OPTIONS] [-- [EXTRA_ANSIBLE_ARGS]]
#
set -e
cd "$(dirname "$0")"

. ./base.sh

# Display combined help for base and reindex-specific options
show_help() {
  echo "Usage: $0 [OPTIONS] [-- [EXTRA_ANSIBLE_ARGS]]"
  echo ""
  echo "Base options:"
  echo "  -e, --env ENV               Environment (local, staging, production)"
  echo "  -i, --inventory FILE        Inventory File"
  echo "  -u, --user USER             Ansible user (default: \$USER)"
  echo "  -h, --help                  Show this help message"
  echo ""
  echo "Reindex specific options:"
  echo "  -s, --source SOURCE         Source index name"
  echo "  -d, --dest DEST             Destination index name (default: mc_search)"
  echo "  -f, --from DATE             Start date for reindexing (format: YYYY-MM-DD)"
  echo "  -t, --to DATE               End date for reindexing (format: YYYY-MM-DD)"
  echo "  -b, --batch-size SIZE       Reindex batch size (default: 1000)"
  exit 0
}

if [ $# -eq 0 ] || [ "$1" = "-h" ] || [ "$1" = "--help" ]; then
  show_help
fi

. ./base.sh

playbook="../playbooks/es-reindex.yml"

# Default values
source_index=""
dest_index="mc_search"
date_from=""
date_to=""
batch_size=1000

# Parse additional arguments
while [ $# -gt 0 ]; do
  case "$1" in
    -s|--source)
      source_index="$2"
      shift 2
      ;;
    -d|--dest)
      dest_index="$2"
      shift 2
      ;;
    -f|--from)
      date_from="$2"
      shift 2
      ;;
    -t|--to)
      date_to="$2"
      shift 2
      ;;
    -b|--batch-size)
      batch_size="$2"
      shift 2
      ;;
    -e|--env|-i|--inventory|-u|--user)
      base_args="$base_args $1 $2"
      shift 2
      ;;
    --)
      shift
      extra_args="$extra_args $*"
      break
      ;;
    *)
      case "$1" in
        -e|--env|-i|--inventory|-u|--user)
          base_args="$base_args $1"
          ;;
        *)
          # Ignore unknown arguments
          ;;
      esac
      shift
      ;;
  esac
done

run_base $base_args

if [ -z "$source_index" ]; then
  echo "Error: Source index (-s, --source) is required."
  exit 1
fi

if [ -z "$date_from" ]; then
  echo "Error: Start date (-f, --from) is required."
  exit 1
fi

if [ -z "$date_to" ]; then
  echo "Error: End date (-t, --to) is required."
  exit 1
fi

ansible_extra_vars="source_index=$source_index dest_index=$dest_index es_reindex_batch_size=$batch_size mc_reindex_date_from=$date_from mc_reindex_date_to=$date_to reindex_continuous=false"

echo "Running one-time reindex with:"
echo "  Inventory: $inventory"
echo "  User: $user"
[ -n "$env" ] && echo "  Environment: $env"
echo "  Source index: $source_index"
echo "  Destination index: $dest_index"
echo "  Batch size: $batch_size"
echo "  Date range: $date_from to $date_to"
[ -n "$extra_args" ] && echo "  Extra args: $extra_args"
echo ""

ansible-playbook "$playbook" $base_args -e "$ansible_extra_vars" $extra_args
