#!/bin/sh

show_help() {
  echo "Usage: $0 [OPTIONS]"
  echo ""
  echo "Reindex specific options:"
  echo "  -l, --local LOCAL                URL of the local Elasticsearch cluster to re-index into"
  echo "  -r, --source-remote REMOTE       URL of the remote Elasticsearch cluster to re-index from"
  echo "  -s, --source-index SOURCE        Source index name"
  echo "  -d, --dest-index DEST            Destination index name (default: mc_search)"
  echo "  -f, --from DATETIME              Start date for re-indexing (format: YYYY-MM-DDTHH:mm:ss.sssZ)"
  echo "  -t, --to DATETIME                End date for re-indexing (format: YYYY-MM-DDTHH:mm:ss.sssZ)"
  echo "  -b, --batch-size SIZE            Number of documents to re-index per batch"
  echo "  -i, --reindex-interval INTERVAL  Used to configure how often the re-indexing job runs via crontab (Default: 1h)"
  echo "  -w, --delay DELAY                Delay buffer between \"now\" and calculated --to time (Default: 2h)"
  echo ""
  echo "Cron management options:"
  echo "  --remove-cron                     Remove the scheduled re-indexing cron job"
  echo "  --update-cron                     Update the existing crontab entry with new configuration parameters"
  exit 0
}

if [ $# -eq 0 ] || [ "$1" = "-h" ] || [ "$1" = "--help" ]; then
  show_help
fi

# Default values
source_remote=""
local=""
source_index=""
dest_index=""
from_datetime=""
to_datetime=""
batch_size=1000
reindex_interval="1h"
delay="2h"
set_cron=false
non_interactive=false

# Parse arguments
while [ $# -gt 0 ]; do
  case "$1" in
   -l|--local)
      local="$2"
      shift 2
      ;;
    -r|--source-remote)
      source_remote="$2"
      shift 2
      ;;
     -s|--source-index)
      source_index="$2"
      shift 2
      ;;
     -d|--dest-index)
      dest_index="$2"
      shift 2
      ;;
     -f|--from)
      from_datetime="$2"
      shift 2
      ;;
      -t|--to)
      to_datetime="$2"
      shift 2
      ;;
      -b|--batch-size)
      batch_size="$2"
      shift 2
      ;;
     -i|--reindex-interval)
      reindex_interval="$2"
      shift 2
      ;;
      -w|--delay)
      delay="$2"
      shift 2
      ;;
     --non-interactive)
      non_interactive=true
      shift
      ;;
     *)
      echo "Unknown option: $1"
      exit 1
      ;;
  esac
done

# Check if we need to schedule the script i.e. from-datetime and to-datetime are set
if [ -n "$from_datetime" ] && [ -n "$to_datetime" ]; then
  set_cron=true
fi

print_reindex_params() {
  echo "PARAMETERS RECEIVED:"
  echo "--source-remote:       $source_remote"
  echo "--local:               $local"
  echo "--source-index:        $source_index"
  echo "--dest-index:          $dest_index"
  echo "--from:                $from_datetime"
  echo "--to:                  $to_datetime"
  echo "--batch-size:          $batch_size"
  echo "--reindex-interval:    $reindex_interval"
  echo "--delay:               $delay"
  echo "set-cron:              $set_cron"
}

start_reindexing_process(){
  if [ "$non_interactive" = true ]; then
    confirm="y"
  else
    printf "Do you want to start the reindexing now? (Y/n): "
    read -r confirm
  fi

  if [ "$confirm" = "y" ] || [ "$confirm" = "Y" ]; then
    echo "Verifying given parameters..."
    check_es_alive "$source_remote" "remote-source"
    check_es_alive "$local" "local"
    check_index_exists "$source_remote" "$source_index"
    check_index_exists "$local" "$dest_index"
    check_datetime_format "$from_datetime" "--form"
    check_datetime_format "$to_datetime" "--to"
    echo "Starting re-indexing process..."
    task_id=$(reindex_from_remote | jq -r '.task')
    echo "Re-index task started with id $task_id"
    if [ "$set_cron" = true ]; then
      echo "Setting cron job..."
      setup_crontab
    fi

    # Append to task list
    append_to_task_list "$task_id" "$from_datetime" "$to_datetime" "running"
  else
    echo "Reindexing aborted."
    exit 1
  fi
}

check_datetime_format() {
  input_datetime="$1"
  date_type="$2"
  regex='^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{3}Z$'

  if ! echo "$input_datetime" | grep -Eq "$regex"; then
    echo "Error: Invalid datetime format: $input_datetime"
    echo "Expected format: YYYY-MM-DDTHH:MM:SS.sssZ"
    exit 1
  else
    echo "Success: Valid [$date_type] date format [$input_datetime]"
  fi
}


check_es_alive() {
    host="$1"
    type="$2"
    response=$(curl -s -o /dev/null -w "%{http_code}" "$host")
    if [ "$response" -ne 200 ]; then
        echo "Error: [$type] elasticsearch at [$host] is not reachable. Status code: $response"
        exit 1
    fi
    echo "Success: [$type] elasticsearch is up and running at [$host]"
}

check_index_exists() {
    host="$1"
    index="$2"
    response=$(curl -s -o /dev/null -w "%{http_code}" "$host/$index")
    if [ "$response" -ne 200 ]; then
        echo "Error: Source index [$index] does not exist."
        exit 2
    fi
    echo "Success: Source index [$index] exists."
}


reindex_from_remote() {
  curl -s -X POST "$local/_reindex?wait_for_completion=false" \
    -H 'Content-Type: application/json' -d @- <<EOF
      {
        "source": {
          "remote": {
            "host": "$source_remote"
          },
          "index": ["$source_index"],
          "size": 1000,
          "query": {
            "range": {
              "indexed_date": {
                "gte": "$from_datetime",
                "lte": "$to_datetime"
              }
            }
          },
          "sort": [
            { "indexed_date": "asc" },
            { "_doc": "asc" }
          ],
          "_source": [
            "article_title",
            "canonical_domain",
            "indexed_date",
            "language",
            "publication_date",
            "text_content",
            "url"
          ]
        },
        "dest": {
          "index": "$dest_index"
        }
      }
EOF
}

setup_crontab() {
    # Get the absolute path of the script
    script_path=$(realpath "$0")
    
    # Create logs directory in user's home directory
    log_dir="$HOME/logs/reindex"
    mkdir -p "$log_dir"
    
    # Set up log file path
    log_file="$log_dir/reindex_$(date +%Y%m%d).log"

    # Parse the interval (e.g., "2h", "10m", "30m", "1h", etc.)
    interval_value=${reindex_interval%[mh]}
    interval_unit="${reindex_interval##*${reindex_interval%?}}"

    case "$interval_unit" in
        "m")
            cron_interval="*/$interval_value * * * *"
            ;;
        "h")
            cron_interval="0 */$interval_value * * *"
            ;;
        *)
            echo "Error: Invalid interval unit. Use 'm' for minutes or 'h' for hours"
            exit 1
            ;;
    esac
    
    # Create the crontab entry with logging
    crontab_entry="$cron_interval $script_path -r $source_remote -l $local -s $source_index -d $dest_index -f $from_datetime -t $to_datetime -b $batch_size -i $reindex_interval -w $delay --non-interactive >> $log_file 2>&1"
    
    # Check if there's an existing crontab entry for this script
    existing_crontab=$(crontab -l 2>/dev/null | grep -F "$script_path")
    
    if [ -n "$existing_crontab" ]; then
        # Remove existing entry
        crontab -l 2>/dev/null | grep -v -F "$script_path" | crontab -
    fi
    
    # Add new entry
    (crontab -l 2>/dev/null; echo "$crontab_entry") | crontab -
    
    echo "Successfully set up crontab entry:"
    echo "$crontab_entry"
    echo "Logs will be written to: $log_file"
}

append_to_task_list() {
  task_id="$1"
  from_time="$2"
  to_time="$3"
  status="$4"

  # Create logs directory in user's home directory
  log_dir="$HOME/logs/reindex"
  mkdir -p "$log_dir"

  # Set up log file path
  log_file="$log_dir/reindex_task.csv"

  # Write header if file doesn't exist
  if [ ! -f "$log_file" ]; then
    echo "task_datetime,task_id,from_time,to_time,status" > "$log_file"
  fi

  now=$(date +"%Y-%m-%d %H:%M:%S")

  # Append single task record
  echo "${now}, ${task_id},${from_time},${to_time},${status}" >> "$log_file"
}

print_reindex_params
start_reindexing_process
