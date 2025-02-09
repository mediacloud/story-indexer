#!/bin/sh

SCRIPT_DIR="$(dirname "$0")"
. "$SCRIPT_DIR/func.sh"

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

print_help(){
    echo ""
    echo "Usage: $0 <start_date> <end_date> <pattern>"
    echo ""
    echo "Description:"
    echo " Outputs a list of files from archives based on a specified date range and matching pattern."
    echo ""
    echo "Arguments:"
    echo " <start_date>            Start date for filtering records (format: YYYY/MM/DD)."
    echo " <end_date>              End date for filtering records (format: YYYY/MM/DD)."
    echo " <pattern>               String pattern used to construct file paths (e.g. 'b2://archives/{pattern}/mchist2022')"
    echo " <output>                The path to the output file where the archive list will be written"
    echo ""
    echo " Example:"
    echo "  $0 2024/12/15 2024/12/31 'b2://archives/{pattern}/mchist2022'"
}

# Handle help flag
if [ "$1" = "-h" ] || [ "$1" = "--help" ]; then
    print_help
    exit 0
fi

# We expect (4) arguments when we want to list files by date
if [ $# -lt 4 ]; then
  print_help
  exit 1
fi

start_date="$1"
end_date="$2"
pattern="$3"

if ! is_valid_date "$start_date" || ! is_valid_date "$end_date"; then
    echo "Error: Invalid date format. Use YYYY/MM/DD" >&2
    exit 1
fi

search_pattern=""
start_date_int=$(convert_date_to_int "$start_date")
end_date_int=$(convert_date_to_int "$end_date")

while [ "$start_date_int" -le "$end_date_int" ]; do
    current_url=$(echo "$pattern" | sed "s|{pattern}|$start_date|g")
    search_pattern="${search_pattern}${current_url} "
    start_date=$(increment_date "$start_date")
    start_date_int=$(convert_date_to_int "$start_date")
done

run_python indexer.scripts.arch-lister $search_pattern -o "$4" -w --rabbitmq-url='-'
