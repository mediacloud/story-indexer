#!/bin/sh

. bin/func.sh

is_valid_date() {
    case "$1" in
        [0-9][0-9][0-9][0-9]/[0-9][0-9]/[0-9][0-9]) return 0 ;;
        *) return 1 ;;
    esac
}
increment_date() {
    date_cmd=$(command -v date)
    case "$(uname)" in
        Darwin)
            "$date_cmd" -j -v+1d -f "%Y/%m/%d" "$1" +"%Y/%m/%d"
            ;;
        Linux)
            "$date_cmd" -d "$1 + 1 day" +"%Y/%m/%d" 2>/dev/null
            ;;
        *)
            echo "Unsupported Environment" >&2
            return 1
            ;;
    esac
}
convert_date_to_int() {
    input_date="$1"
    if [ "$(uname)" = "Darwin" ]; then
        date -j -f "%Y/%m/%d" "$input_date" +"%Y%m%d" 2>/dev/null
    elif [ "$(uname)" = "Linux" ]; then
        date -d "$input_date" +"%Y%m%d" 2>/dev/null
    else
        echo "Unsupported OS" >&2
        return 1
    fi
}

print_erase_help(){
    echo "Usage: $0 --erase <input_file_path> --elasticsearch-hosts=<host> --indices=<index1,index2,...>"
    echo ""
    echo "Description:"
    echo " Removes documents from Elasticsearch based on URLs provided from an input file"
    echo ""
    echo "Options:"
    echo "  --erase <input_file_path>       Path to the input file containing records to delete. This supports path to remote store i.e. S3,or B2, local file or a file containing a list URLs to delete (must start with @ to specify that it's an indirect file)"
    echo "  --elasticsearch-hosts=<host>    Comma-separated list of Elasticsearch hosts (e.g., http://localhost:9200)."
    echo "  --indices=<index1,index2,...>   Comma-separated list of indices to target in Elasticsearch."
    echo ""
    echo ""
    echo "Optional:"
    echo "  --batch-delete                  Perform batch deletions in Elasticsearch instead of single requests (default: False)."
    echo "  --fetch-batch-size=<size>       Number of records to fetch in each batch for deletion (default: 1000)."
    echo "  --dry-run                       Simulate the operation without making changes to Elasticsearch."
    echo ""
    echo "Example:"
    echo "  $0 --erase @arch-eraser/output.txt --elasticsearch-hosts=http://localhost:9200 --indices=index1,index2 --batch-delete --fetch-batch-size=5000 --dry-run"
    exit 1
}

print_generate_erase_list_help(){
    echo "Usage: $0 --generate-erase-list [start_date] [end_date] [pattern] --output-file=[path]" >&2
    echo "Usage: $0 --generate-erase-list <start_date> <end_date> <pattern> --output-file=<path>"
    echo ""
    echo "Description:"
    echo "  Generates a list of records to be erased based on the specified date range and pattern."
    echo ""
    echo "Arguments:"
    echo "  <start_date>                    Start date for filtering records (format: YYYY/MM/DD)."
    echo "  <end_date>                      End date for filtering records (format: YYYY/MM/DD)."
    echo "  <pattern>                       String pattern used to construct file paths (e.g., 'b2://archives/{pattern}/mchist2022')"
    echo "  --output-file=<path>            Path to save the generated erase list."
    echo ""
    echo "Requirements:"
    echo "  - The start and end dates must be in the format YYYY/MM/DD."
    echo "  - The --output-file argument is required and must specify a valid path."
    echo ""
    echo "Example:"
    echo "  $0 --generate-erase-list 2024/12/15 2024/12/31 'b2://archives/{pattern}/mchist2022' --output-file=arch-eraser/erase-list.txt"
    exit 1
}

print_help(){
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  --generate-erase-list           Generates an erase list based on date range and pattern."
    echo "  --erase                         Deletes records from Elasticsearch."
    echo "  -h, --help                      Show this help message."
    echo "  -h --generate-erase-list        Show help for the --generate-erase-list action."
    echo "  -h --erase                      Show help for the --erase action."
    exit 0
}

# Handle help flag
# Handle help flag
# Handle help flag
if [ "$1" = "-h" ] || [ "$1" = "--help" ]; then
    if [ "$2" = "--generate-erase-list" ]; then
        print_generate_erase_list_help
    elif [ "$2" = "--erase" ]; then
        print_erase_help
    else
        print_help
    fi
    exit 0
fi

action="$1"

if [ "$action" = "--generate-erase-list" ]; then
  # We expect (4) arguments when we want to generate erase list
  if [ $# -lt 5 ]; then
    print_generate_erase_list_help
  fi
  start_date="$2"
  end_date="$3"
  pattern="$4"
  shift 4
  other_params="$* --test"
  if ! is_valid_date "$start_date" || ! is_valid_date "$end_date"; then
      echo "Error: Invalid date format. Use YYYY/MM/DD" >&2
      exit 1
  fi
  output_string=""
  start_date_int=$(convert_date_to_int "$start_date")
  end_date_int=$(convert_date_to_int "$end_date")
  while [ "$start_date_int" -le "$end_date_int" ]; do
      current_url=$(echo "$pattern" | sed "s|{pattern}|$start_date|g")
      output_string="${output_string}${current_url} "
      start_date=$(increment_date "$start_date")
      start_date_int=$(convert_date_to_int "$start_date")
  done
elif [ "$action" = "--erase" ]; then
  case "$*" in
       *--elasticsearch-hosts=* ) has_hosts=true ;;
       * ) has_hosts=false ;;
   esac

   case "$*" in
       *--indices=* ) has_indices=true ;;
       * ) has_indices=false ;;
   esac

    if [ "$has_hosts" = false ] || [ "$has_indices" = false ]; then
      print_erase_help
    fi
   output_string=$2
   shift 2
   other_params="$*"
else
   echo "Error: Invalid argument. The first argument must be either '--help', '-h', '--generate-erase-list', or '--erase'" >&2
   exit 1
fi
echo  ${output_string} ${other_params}
run_python indexer.scripts.arch-eraser ${output_string} ${other_params} "--rabbitmq-url='-'"
