#!/bin/sh

# -----------------------------
#          Functions
# -----------------------------
show_help() {
  echo "Usage: $0 [OPTIONS]"
  echo ""
  echo "Reindex specific options:"
  echo "  -l, --dest-remote-host DEST_URL      URL of the destination Elasticsearch cluster to re-index into"
  echo "  -r, --source-remote-host SRC_URL     URL of the remote Elasticsearch cluster to re-index from"
  echo "  -s, --source-index SOURCE            Source index name"
  echo "  -d, --dest-index DEST                Destination index name (default: mc_search)"
  echo "  -f, --from DATETIME                  Start date for re-indexing (format: YYYY-MM-DDTHH:mm:ss.sssZ)"
  echo "  -t, --to DATETIME                    End date for re-indexing (format: YYYY-MM-DDTHH:mm:ss.sssZ)"
  echo "  -b, --batch-size SIZE                Number of documents to re-index per batch"
  echo "  -i, --reindex-interval INTERVAL      Used to configure how often the re-indexing job runs via crontab (Default: 1h)"
  echo "  -w, --delay DELAY                    Delay buffer between \"now\" and calculated --to time (Default: 2h)"
  echo ""
  echo "Cron management options:"
  echo "  --remove-cron                     Remove the scheduled re-indexing cron job"
  echo "  --update-cron                     Update the existing crontab entry with new configuration parameters"
  exit 0
}

parse_args(){
  # Set default values
  source_remote_host=""
  dest_remote_host=""
  source_index=""
  dest_index=""
  from_datetime=""
  to_datetime=""
  batch_size=1000
  reindex_interval="1h"
  delay="2h"
  set_cron=false
  non_interactive=false
  action="re-index"

  # Parse arguments
  while [ $# -gt 0 ]; do
      case "$1" in
      -l | --dest-url) dest_remote_host="$2"; shift 2 ;;
      -r | --source-remote-host) source_remote_host="$2"; shift 2 ;;
      -s | --source-index) source_index="$2"; shift 2 ;;
      -d | --dest-index) dest_index="$2"; shift 2 ;;
      -f | --from) from_datetime="$2"; shift 2 ;;
      -t | --to) to_datetime="$2"; shift 2;;
      -b | --batch-size) batch_size="$2"; shift 2 ;;
      -i | --reindex-interval) reindex_interval="$2"; shift 2 ;;
      -w | --delay) delay="$2"; shift 2 ;;
      --non-interactive) non_interactive=true; shift ;;
      --remove-cron) action="remove-cron"; shift ;;
      --update-cron) action="update-cron"; shift ;;
      -h|--help) show_help ;;
      *) echo "Unknown option: $1"; exit 1 ;;
      esac
  done

  # Logic: If from is set but to is not, or non-interactive is true, then we are running a cron job
  if { [ -n "$from_datetime" ] && [ -z "$to_datetime" ]; } || [ "$non_interactive" = true ]; then
    set_cron=true
    to_datetime=$(calculate_to_date)
  fi

  # Other default values
  log_dir="/srv/data/elasticsearch/reindex"

}

print_reindex_params() {
  echo "PARAMETERS RECEIVED:"
  echo "--source-remote-host:  $source_remote_host"
  echo "--dest-url:            $dest_remote_host"
  echo "--source-index:        $source_index"
  echo "--dest-index:          $dest_index"
  echo "--from:                $from_datetime"
  echo "--to:                  $to_datetime"
  echo "--batch-size:          $batch_size"
  echo "--reindex-interval:    $reindex_interval"
  echo "--delay:               $delay"
  echo "--set-cron:            $set_cron"
}

check_datetime_format() {
  input_datetime="$1"
  date_type="$2"
  regex='^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{3}Z$'

  if ! echo "$input_datetime" | grep -Eq "$regex"; then
    echo "Error: Invalid [$date_type] datetime format: [$input_datetime]"
    exit 1
  else
    echo "Success: Valid [$date_type] datetime format [$input_datetime]"
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

append_to_task_list() {
  task_id="$1"
  from_time="$2"
  to_time="$3"
  status="$4"
  error_description="$5"

  # Create logs directory
  mkdir -p "$log_dir"

  # Set up log file path
  log_file="$log_dir/tasks_status.csv"

  # Write header if file doesn't exist
  if [ ! -f "$log_file" ]; then
    echo "task_datetime,task_id,from_time,to_time,status,error_description" >"$log_file"
  fi

  now=$(date +"%Y-%m-%dT%H:%M:%S")

  # Append single task record
  echo "${now},${task_id},${from_time},${to_time},${status},${error_description}" >>"$log_file"
}

get_last_task_details() {
  csv_file="$1"
  delimiter="${2:-,}"

  if [ ! -f "$csv_file" ]; then
    echo "Error: File '$csv_file' not found"
    return 1
  fi

  # Get the last row
  last_row=$(tail -n 1 "$csv_file")

  # Extract each column value
  last_task_id=$(echo "$last_row" | cut -d"$delimiter" -f2)
  last_from_time=$(echo "$last_row" | cut -d"$delimiter" -f3)
  last_to_time=$(echo "$last_row" | cut -d"$delimiter" -f4)
}

check_task_status_in_es() {
  TASK_ID="$1"
  result=$(curl -s "${dest_remote_host}/_tasks/${TASK_ID}" | jq -r '.completed // false, .error.reason // ""')
  is_completed=$(echo "$result" | sed -n '1p')
  error_description=$(echo "$result" | sed -n '2p')
}

calculate_to_date() {
    value=$(echo "$delay" | sed -E 's/[^0-9].*//')
    unit=$(echo "$delay" | sed -E 's/[0-9]+//')

    case "$unit" in
        h) unit="hours" ;;
        m) unit="minutes" ;;
        *)
          echo "Error: Unsupported interval time unit: $unit" >&2
          exit 1
      ;;
    esac
  date  -d "$value $unit ago" +"%Y-%m-%dT%H:%M:%S.%3NZ"
}

reindex_from_remote() {
  curl -s -X POST "$dest_remote_host/_reindex?wait_for_completion=false" \
    -H 'Content-Type: application/json' -d @- <<EOF
      {
        "source": {
          "remote": {
            "host": "$source_remote_host"
          },
          "index": ["$source_index"],
          "size": $batch_size,
          "query": {
            "range": {
              "indexed_date": {
                "gt": "$from_datetime",
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
  start_date=$1

  # Get the absolute path of the script
  script_path=$(realpath "$0")

  # Create logs directory
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
  crontab_entry="$cron_interval $script_path -r $source_remote_host -l $dest_remote_host -s $source_index -d $dest_index -f $start_date -b $batch_size -i $reindex_interval -w $delay --non-interactive >> $log_file 2>&1"

  # Check if there's an existing crontab entry for this script
  existing_crontab=$(crontab -l 2>/dev/null | grep -F "$script_path")

  if [ -n "$existing_crontab" ]; then
    # Remove existing entry
    crontab -l 2>/dev/null | grep -v -F "$script_path" | crontab -
  fi

  # Add new entry
  (
    crontab -l 2>/dev/null
    echo "$crontab_entry"
  ) | crontab -

  echo "Successfully set up crontab entry:"
  echo "$crontab_entry"
  echo "Logs will be written to: $log_file"
}

start_reindexing_process() {
  if [ "$non_interactive" = true ]; then
    confirm="y"
  else
    printf "Do you want to start the reindexing now? (Y/n): "
    read -r confirm
    if [ "$confirm" != "y" ] && [ "$confirm" != "Y" ]; then
      echo "Reindexing aborted."
      exit 1
    fi
  fi

  log_file="$log_dir/tasks_status.csv"
  get_last_task_details "$log_file"
  echo "Last task_id: $last_task_id"

  # Get the task status from Elasticsearch
  if [ "$last_task_id" != "" ]; then
    check_task_status_in_es "$last_task_id"
  else
    is_first_run=true
  fi

  # Don't start a task if the last task is not completed successfully
  if [ "$is_completed" = true ] || [ "$is_first_run" = true ]; then
    # Check if the completed task has no error
    if [ "$error_description" != "" ]; then
      echo "Error: Aborting re-indexing because ES reports task with id $last_task_id failed"
      append_to_task_list "$last_task_id" "$from_datetime" "$to_datetime" "failed" "$error_description"
      exit 1
    fi

    # Append last task complete status in log file
    if [ "$is_completed" = true ]; then
      append_to_task_list "$last_task_id" "$last_from_time" "$last_to_time" "completed"
    fi

    echo "Verifying given parameters..."
    check_es_alive "$source_remote_host" "source"
    check_es_alive "$dest_remote_host" "destination"
    check_index_exists "$source_remote_host" "$source_index"
    check_index_exists "$dest_remote_host" "$dest_index"
    check_datetime_format "$from_datetime" "--from"
    check_datetime_format "$to_datetime" "--to"
    echo "Starting re-indexing process..."
    task_id=$(reindex_from_remote | jq -r '.task')
    echo "Re-index task started with id $task_id"
    if [ "$set_cron" = true ]; then
      echo "Setting up cron job..."
      # Always use the calculated --to-datetime as the --from-datetime on next run
      setup_crontab "$to_datetime"
    fi
    append_to_task_list "$task_id" "$from_datetime" "$to_datetime" "running"
  else
    echo "Warning: Can not start another re-index task with id $last_task_id is still running..."
    append_to_task_list "$last_task_id" "$from_datetime" "$to_datetime" "running"
  fi
}

remove_crontab() {
  # Get the absolute path of the script
  script_path=$(realpath "$0")

  printf "Do you want to remove this script from the crontab ? [Y/n]: "
  read -r confirm
  if [ "$confirm" != "y" ] && [ "$confirm" != "Y" ]; then
    echo "Cron removal aborted."
    exit 1
  fi
  
  # Remove any existing crontab entries for this script
  existing_crontab=$(crontab -l 2>/dev/null | grep -F "$script_path")
  if [ -n "$existing_crontab" ]; then
    crontab -l 2>/dev/null | grep -v -F "$script_path" | crontab -
    echo "Successfully removed crontab entry for $script_path"
  else
    echo "No crontab entry found for $script_path"
  fi
}

update_crontab() {
  # Get the absolute path of the script
  script_path=$(realpath "$0")
  
  # Get existing crontab entry
  existing_crontab=$(crontab -l 2>/dev/null | grep -F "$script_path")
  
  if [ -z "$existing_crontab" ]; then
    echo "Error: No existing crontab entry found for $script_path"
    exit 1
  fi
  
  # Extract parameters from existing crontab entry
  existing_source_remote_host=$(echo "$existing_crontab" | grep -o -- '-r [^ ]*' | cut -d' ' -f2)
  existing_dest_remote_host=$(echo "$existing_crontab" | grep -o -- '-l [^ ]*' | cut -d' ' -f2)
  existing_source=$(echo "$existing_crontab" | grep -o -- '-s [^ ]*' | cut -d' ' -f2)
  existing_dest=$(echo "$existing_crontab" | grep -o -- '-d [^ ]*' | cut -d' ' -f2)
  existing_from=$(echo "$existing_crontab" | grep -o -- '-f [^ ]*' | cut -d' ' -f2)
  existing_batch=$(echo "$existing_crontab" | grep -o -- '-b [^ ]*' | cut -d' ' -f2)
  existing_interval=$(echo "$existing_crontab" | grep -o -- '-i [^ ]*' | cut -d' ' -f2)
  existing_delay=$(echo "$existing_crontab" | grep -o -- '-w [^ ]*' | cut -d' ' -f2)

  
  # Use new parameters if provided, otherwise keep existing ones
  source_remote_host=${source_remote_host:-$existing_source_remote_host}
  dest_remote_host=${dest_remote_host:-$existing_dest_remote_host}
  source_index=${source_index:-$existing_source}
  dest_index=${dest_index:-$existing_dest}
  from_datetime=${from_datetime:-$existing_from}
  batch_size=${batch_size:-$existing_batch}
  reindex_interval=${reindex_interval:-$existing_interval}
  delay=${delay:-$existing_delay}

  # Reuse setup_crontab with the updated parameters
  setup_crontab "$from_datetime"
}

main() {
   if [ $# -eq 0 ]; then
    show_help
    exit 0
  fi

  parse_args "$@"
  if [ "$action" = "remove-cron" ]; then
    remove_crontab
    exit 0
  elif [ "$action" = "update-cron" ]; then
    update_crontab
    exit 0
  elif [ "$action" = "re-index" ]; then
    print_reindex_params
    start_reindexing_process
  fi
}

# -----------------------------
#          Run Script
# -----------------------------
main "$@"
