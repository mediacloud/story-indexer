
#!/bin/sh
### Note: Decoupled this from Ansible since its a one-off re-indexing script and using Ansible requires
#### extra management of handling passwords for sudo privileges

# Run continuous Elasticsearch reindexing via API calls
# Usage: scripts/es-reindex-continuous.sh [OPTIONS]
#
set -e

STATE_DIR="$HOME/elasticsearch_reindex"
mkdir -p "$STATE_DIR"

show_help() {
  echo "Usage: $0 [OPTIONS]"
  echo ""
  echo "Options:"
  echo "  -h, --host HOST             Elasticsearch host (required)"
  echo "  -s, --source SOURCE         Source index name (required)"
  echo "  -d, --dest DEST             Destination index name (default: mc_search)"
  echo "  -f, --from DATE             Start date for first run (format: YYYY-MM-DD, required for first run)"
  echo "  -v, --interval HOURS        Continuous re-index interval in hours (default: 2)"
  echo "  -b, --batch-size SIZE       Reindex batch size (default: 1000)"
  echo "  --help                      Show this help message"
  echo ""
  echo "Examples:"
  echo "  $0 -h localhost:9200 -s mc_search-000001 -d mc_search -f 2024-01-01"
  exit 0
}

if [ $# -eq 0 ] || [ "$1" = "--help" ]; then
  show_help
fi

# Default values
es_host=""
source_index=""
dest_index="mc_search"
date_from=""
reindex_interval=2
batch_size=1000

# Parse arguments
while [ $# -gt 0 ]; do
  case "$1" in
    -h|--host)
      es_host="$2"
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
    *)
      echo "Unknown option: $1"
      echo "Use --help for usage information."
      exit 1
      ;;
  esac
done

# Validate required parameters
if [ -z "$es_host" ]; then
  echo "Error: Elasticsearch host (-h, --host) is required."
  exit 1
fi

if [ -z "$source_index" ]; then
  echo "Error: Source index (-s, --source) is required."
  exit 1
fi

# Manage state for continuous-reindex
state_file="$STATE_DIR/${source_index}_${dest_index}.state"

# Function to check task status
check_task_status() {
  local task_id=$1
  curl -s -X GET "http://$es_host/_tasks/$task_id"
}

# Function to get current timestamp (cross-platform)
get_current_timestamp() {
  date +%Y-%m-%dT%H:%M:%S%z
}

# Function to execute a single reindex batch
execute_reindex_batch() {
  local from_date=$1
  local to_date=$2

  # Build reindex API request body for this batch
  local batch_body=$(cat <<EOF
{
    "source": {
      "remote": {
            "host": "http://host.docker.internal:9220"
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

  echo "Starting reindex batch: $from_date to $to_date"

  # Start async reindex and get task_id
  local response=$(curl -s -X POST "http://$es_host/_reindex?wait_for_completion=false" \
       -H "Content-Type: application/json" \
       -d "$batch_body")

  local task_id=$(echo "$response" | grep -o '"task":"[^"]*"' | cut -d'"' -f4)

  if [ -z "$task_id" ]; then
    echo "Failed to get task_id. Response:"
    echo "$response"
    return 1
  fi

  echo "Reindex task started with ID: $task_id"

  # Monitor task status
  while true; do
    local task_status=$(check_task_status "$task_id")
    local completed=$(echo "$task_status" | grep -o '"completed":[^,}]*' | cut -d':' -f2)

    if [ "$completed" = "true" ]; then
      echo "Task $task_id completed"

      # Check for failures
      local failures=$(echo "$task_status" | grep -o '"failures":\[[^\]]*\]')
      if [ "$failures" = '"failures":[]' ] || [ -z "$failures" ]; then
        # Extract metrics from response
        local total=$(echo "$task_status" | grep -o '"total":[0-9]*' | cut -d':' -f2)
        local created=$(echo "$task_status" | grep -o '"created":[0-9]*' | cut -d':' -f2)

        echo "Batch completed successfully:"
        echo "  Total documents processed: $total"
        echo "  Documents created: $created"

        # Update state file on successful completion
        echo "$to_date" > "$state_file"
        echo "State updated. Next batch will start from: $to_date"

        return 0
      else
        echo "Task completed with failures:"
        echo "$failures"
        return 1
      fi
    else
      # Task still running, wait before checking again
      sleep 10
      echo "Task $task_id still running..."
    fi
  done
}

# Main continuous reindexing loop
echo "Starting continuous reindexing process..."
echo "Press Ctrl+C to stop"

while true; do
  current_time=$(get_current_timestamp)

  if [ -f "$state_file" ]; then
    # Subsequent runs - get last end time from state file as new start time
    batch_from=$(cat "$state_file")
    batch_to="$current_time"
  else
    if [ -z "$date_from" ]; then
      echo "Error: First continuous run requires --from date"
      exit 1
    fi
    batch_from="$date_from"
    batch_to="$current_time"
  fi

  echo ""
  echo "=== Starting new batch ==="
  echo "Elasticsearch host: $es_host"
  echo "Source index: $source_index"
  echo "Destination index: $dest_index"
  echo "Batch size: $batch_size"
  echo "Date range: $batch_from to $batch_to"

  # Execute the batch
  if execute_reindex_batch "$batch_from" "$batch_to"; then
    echo "Batch completed successfully"

    # Wait for the specified interval before next batch
    echo "Waiting ${reindex_interval} hours before next batch..."
    sleep $((reindex_interval * 3600))
  else
    echo "Batch failed. Exiting."
    exit 1
  fi
done

