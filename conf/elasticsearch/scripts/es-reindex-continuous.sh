#!/bin/sh
### Note: Decoupled this from Ansible since its a one-off re-indexing script and using Ansible requires
#### extra management of handling passowrds for sudo privileges
set -e

STATE_DIR="$HOME/elasticsearch_reindex"
mkdir -p "$STATE_DIR"

show_help() {
  echo "Usage: $0 [OPTIONS]"
  echo ""
  echo "Options:"
  echo "  -h, --host HOST             Elasticsearch host (required)"
  echo "  -r, --remote HOST      Elasticsearch remote reindexing host (required)"
  echo "  -s, --source SOURCE         Source index name (required)"
  echo "  -d, --dest DEST             Destination index name (default: mc_search)"
  echo "  -f, --from DATE             Start date for first run (format: YYYY-MM-DD, required for first run)"
  echo "  -v, --interval HOURS        Continuous re-index interval in hours (default: 6)"
  echo "  -b, --batch-size SIZE       Reindex batch size (default: 1000)
  --setup-cron                Set up cron job using reindex interval"
  echo "  --help                      Show this help message"
  echo ""
  echo "Examples:"
  echo "  # First run:"
  echo "  $0 -h localhost:9200 -r http://ramos.angwin:9200 -s mc_search-000001 -d mc_search -f 2024-01-01"
  echo "  # Set up cron job using current reindex interval (2 hours):"
  echo "  $0 -h localhost:9200  -r http://ramos.angwin:9200 -s mc_search-000001 -v 2 --setup-cron"
  exit 0
}

if [ $# -eq 0 ] || [ "$1" = "--help" ]; then
  show_help
fi

# Default values
es_host=""
remote_host=""
source_index=""
dest_index="mc_search"
date_from=""
reindex_interval=6
batch_size=1000
setup_cron=false

# Parse arguments
while [ $# -gt 0 ]; do
  case "$1" in
    -h|--host)
      es_host="$2"
      shift 2
      ;;
    -r|--remote)
      remote_host="$2"
      shift 2
      ;;
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
    -v|--interval)
      reindex_interval="$2"
      shift 2
      ;;
    -b|--batch-size)
      batch_size="$2"
      shift 2
      ;;
    --setup-cron)
      setup_cron=true
      shift 1
      ;;
    *)
      echo "Unknown option: $1"
      echo "Use --help for usage information."
      exit 1
      ;;
  esac
done

if [ -z "$es_host" ]; then
  echo "Error: Elasticsearch host (-h, --host) is required."
  exit 1
fi
if [ -z "$remote_host" ]; then
  echo "Error: Remote Elasticsearch host (-r, --remote) is required."
  exit 1
fi
if [ -z "$source_index" ]; then
  echo "Error: Source index (-s, --source) is required."
  exit 1
fi
# Setup cron
if [ "$setup_cron" = true ]; then
  script_path="$(dirname "$0")/$(basename "$0")"
  cron_command="$script_path -h $es_host -r $remote_host -s $source_index -d $dest_index -v $reindex_interval -b $batch_size"

  # Calculate cron schedule based on reindex_interval
  if [ "$reindex_interval" -lt 1 ] || [ "$reindex_interval" -gt 23 ]; then
    echo "Error: Reindex interval must be between 1 and 23 hours"
    exit 1
  fi

  cron_schedule="0 */$reindex_interval * * *"

  # Add to crontab
  (crontab -l 2>/dev/null | grep -v "$script_path"; echo "$cron_schedule $cron_command >> $STATE_DIR/reindex.log 2>&1") | crontab -

  echo "Cron job set up successfully:"
  echo "Schedule: Every $reindex_interval hour(s)"
  echo "Command: $cron_command"
  echo "Logs: $STATE_DIR/reindex.log"
  echo ""
  echo "To remove the cron job later, run: crontab -e"
  exit 0
fi

state_file="$STATE_DIR/${source_index}_${dest_index}.state"

check_task_status() {
  local task_id=$1
  curl -s -X GET "http://$es_host/_tasks/$task_id"
}

get_current_timestamp() {
  date +%Y-%m-%dT%H:%M:%S%z
}

execute_reindex_batch() {
  local from_date=$1
  local to_date=$2

  local batch_body=$(cat <<EOF
{
    "source": {
      "remote": {
            "host": "$remote_host"
        },
      "index": ["$source_index"],
      "size": $batch_size,
      "query": {
        "range": {
          "indexed_date": {
            "gte": "$from_date",
            "lte": "$to_date"
          }
        }
      },
      "sort": [
        { "indexed_date": "asc" },
         { "_doc": "asc" }
      ],
      "_source": ["article_title", "canonical_domain", "indexed_date", "language", "publication_date", "text_content", "url"]
    },
    "dest": {
      "index": "mc_search"
    }
  }
EOF
)

  echo "$(date): Starting reindex batch: $from_date to $to_date"

  local response=$(curl -s -X POST "http://$es_host/_reindex?wait_for_completion=false" \
       -H "Content-Type: application/json" \
       -d "$batch_body")

  local task_id=$(echo "$response" | grep -o '"task":"[^"]*"' | cut -d'"' -f4)

  if [ -z "$task_id" ]; then
    echo "$(date): Failed to get task_id. Response:"
    echo "$response"
    return 1
  fi

  echo "$(date): Reindex task started with ID: $task_id"

  # Monitor task status with timeout
  local timeout_count=0
  local max_timeout=360

  while true; do
    local task_status=$(check_task_status "$task_id")
    local completed=$(echo "$task_status" | grep -o '"completed":[^,}]*' | cut -d':' -f2)

    if [ "$completed" = "true" ]; then
      echo "$(date): Task $task_id completed"

      local failures=$(echo "$task_status" | grep -o '"failures":\[[^\]]*\]')
      if [ "$failures" = '"failures":[]' ] || [ -z "$failures" ]; then
        local total=$(echo "$task_status" | grep -o '"total":[0-9]*' | cut -d':' -f2)
        local created=$(echo "$task_status" | grep -o '"created":[0-9]*' | cut -d':' -f2)

        echo "$(date): Batch completed successfully:"
        echo "  Total documents processed: $total"
        echo "  Documents created: $created"

        echo "$to_date" > "$state_file"
        echo "$(date): State updated. Next batch will start from: $to_date"
        return 0
      else
        echo "$(date): Task completed with failures:"
        echo "$failures"
        return 1
      fi
    else
      timeout_count=$((timeout_count + 1))
      if [ $timeout_count -ge $max_timeout ]; then
        echo "$(date): Task $task_id timed out after 1 hour"
        return 1
      fi

      sleep 10
      if [ $((timeout_count % 30)) -eq 0 ]; then  # Log every 5 minutes
        echo "$(date): Task $task_id still running... (${timeout_count}0 seconds elapsed)"
      fi
    fi
  done
}

# Main execution (single run for cron)
current_time=$(get_current_timestamp)

if [ -f "$state_file" ]; then
  batch_from=$(cat "$state_file")
  batch_to="$current_time"
else
  if [ -z "$date_from" ]; then
    echo "$(date): Error: First run requires --from date"
    exit 1
  fi
  batch_from="$date_from"
  batch_to="$current_time"
fi

echo "$(date): === Starting reindex batch ==="
echo "$(date): Elasticsearch host: $es_host"
echo "$(date): Source index: $source_index"
echo "$(date): Destination index: $dest_index"
echo "$(date): Batch size: $batch_size"
echo "$(date): Date range: $batch_from to $batch_to"

if execute_reindex_batch "$batch_from" "$batch_to"; then
  echo "$(date): Batch completed successfully"
  exit 0
else
  echo "$(date): Batch failed"
  exit 1
fi
