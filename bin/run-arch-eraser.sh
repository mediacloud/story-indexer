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

if [ $# -lt 3 ]; then
    echo "Usage: $0 [start_date] [end_date] [pattern]" >&2
    exit 1
fi

start_date="$1"
end_date="$2"
pattern="$3"
shift 3
other_params="$*"

if ! is_valid_date "$start_date" || ! is_valid_date "$end_date"; then
    echo "Error: Invalid date format. Use YYYY/MM/DD" >&2
    exit 1
fi

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

output_string=""
start_date_int=$(convert_date_to_int "$start_date")
end_date_int=$(convert_date_to_int "$end_date")

while [ "$start_date_int" -le "$end_date_int" ]; do
    current_url=$(echo "$pattern" | sed "s|{pattern}|$start_date|g")
    output_string="${output_string}${current_url} "
    start_date=$(increment_date "$start_date")
    start_date_int=$(convert_date_to_int "$start_date")
done

run_python indexer.scripts.arch-eraser ${output_string} ${other_params}
