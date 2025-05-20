#!/bin/sh
# Run Elasticsearch playbook to reindex data
# Usage: scripts/es-reindex.sh [OPTIONS] [-- [EXTRA_ANSIBLE_ARGS]]
#
# Additional options:
#   -s, --source SOURCE         Source index name
#   -d, --dest DEST             Destination index name (default: mc_search)
#   -f, --from DATE             Start date for reindexing (format: YYYY-MM-DD)
#   -t, --to DATE               End date for reindexing (format: YYYY-MM-DD)
#   -c, --continuous            Enable continuous reindexing (every 12 hours)
#   -b, --batch-size SIZE       Reindex batch size (default: 1000)
set -e
cd "$(dirname "$0")"

. ./base.sh "$@"
run_base "$@"

playbook="../playbooks/es-reindex.yml"

source_index=""
dest_index="mc_search"
date_from=""
date_to=""
continuous=false
batch_size=1000

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
    -c|--continuous)
      continuous=true
      shift
      ;;
    -b|--batch-size)
      batch_size="$2"
      shift 2
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

if [ -z "$source_index" ]; then
  echo "Error: Source index (-s, --source) is required."
  exit 1
fi

extra_vars="source_index=$source_index dest_index=$dest_index es_reindex_batch_size=$batch_size"

if $continuous; then
  extra_vars="$extra_vars mc_reindex_continuous=true"
else
  if [ -z "$date_from" ]; then
    echo "Error: Start date (-f, --from) is required for non-continuous reindexing."
    exit 1
  fi

  if [ -z "$date_to" ]; then
    echo "Error: End date (-t, --to) is required for non-continuous reindexing."
    exit 1
  fi

  extra_vars="$extra_vars mc_reindex_date_from=$date_from mc_reindex_date_to=$date_to mc_reindex_continuous=false"
fi

extra_args="$extra_args -e \"$extra_vars\""

echo "Running playbook with:"
echo "  Inventory: $inventory"
echo "  User: $user"
[ -n "$env" ] && echo "  Environment: $env"
echo "  Source index: $source_index"
echo "  Destination index: $dest_index"
echo "  Batch size: $batch_size"
if $continuous; then
  echo "  Mode: Continuous (every 12 hours)"
else
  echo "  Mode: One-time reindex"
  echo "  Date range: $date_from to $date_to"
fi
[ -n "$extra_args" ] && echo "  Extra args: $extra_args"
echo ""

ansible-playbook "$playbook" $base_args $extra_args
