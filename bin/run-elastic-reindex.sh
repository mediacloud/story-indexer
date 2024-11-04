#!/bin/sh

# Elasticsearch Reindexing Script
# This script performs a reindexing operation from a source index to a destination index in Elasticsearch,
# with the following checks:
# 1. Ensures Elasticsearch is running and reachable.
# 2. Verifies that the source index exists.
# 3. Checks that the destination index does not already exist.
# 4. Initiates the reindex task asynchronously and retrieves the task ID.
# 5. Logs output for each step and provides appropriate error handling.
#

# Usage:
# Modify the variables ES_HOST, SOURCE_INDEX, and DEST_SUFFIX and MAX_DOCS as needed.
# Run this script using: sh reindex.sh
# ./run-elastic-reindex.sh @
#

display_help() {
    echo "Elasticsearch Reindexing Script"
    echo
    echo "Usage: sh run-elastic-reindex.sh [-h] [-e ES_HOST] -s SOURCE_INDEX... -d DEST_SUFFIX"
    echo "Example: sh run-elastic-reindex.sh -e http://localhost:9200 -s mc_search-000001 mc_search-000002 -d reindexed -m 1000"
    echo
    echo "Arguments:"
    echo "  -h            Show help information."
    echo "  -e ES_HOST    Optional. The URL of the Elasticsearch host (default: http://localhost:9200)."
    echo "  -s SOURCE_INDEX Required. One or more source indices to reindex from (space-separated)."
    echo "  -d DEST_SUFFIX Required. Suffix for the destination index names."
    echo "  -m MAX_DOCS   Optional. The maximum number of documents to re-index. Must be a positive integer."
    echo
}

ES_HOST="http://localhost:9210"
OP_TYPE="create"  # Operation type for reindex, could be `create` or `index`
SOURCE_INDEXES=() # Array to hold source indices
DEST_SUFFIX="" #Suffix destination indexes
MAX_DOCS="" # Maximum number of documents to reindex

while getopts ":he:s:d:m:q:" opt; do
    case $opt in
        h)
            display_help
            exit 0
            ;;
        e)
            ES_HOST=$OPTARG
            ;;
        s)
            shift $((OPTIND - 2))
            while [[ "$1" != -* && -n "$1" ]]; do
                SOURCE_INDEXES+=("$1")
                shift
            done
            OPTIND=1
            ;;
        d)
            DEST_SUFFIX=$OPTARG
            ;;
        m)
            MAX_DOCS=$OPTARG
            ;;
        q)
            QUERY=$OPTARG
            ;;
        \?)
            echo "Invalid option: -$OPTARG" >&2
            display_help
            exit 1
            ;;
        :)
            echo "Option -$OPTARG requires an argument." >&2
            display_help
            exit 1
            ;;
    esac
done

if [ ${#SOURCE_INDEXES[@]} -eq 0 ] || [ -z "$DEST_SUFFIX" ]; then
    echo "Error: At least one SOURCE_INDEX and a DEST_SUFFIX are required."
    display_help
    exit 5
fi

check_es_alive() {
    response=$(curl -s -o /dev/null -w "%{http_code}" "$ES_HOST")
    if [ "$response" -ne 200 ]; then
        echo "Error: Elasticsearch is not reachable. Status code: $response"
        exit 1
    fi
    echo "Elasticsearch is up and running."
}

check_source_index_exists() {
    for index in "${SOURCE_INDEXES[@]}"; do
        response=$(curl -s -o /dev/null -w "%{http_code}" "$ES_HOST/$index")
        if [ "$response" -ne 200 ]; then
            echo "Error: Source index '$index' does not exist."
            exit 2
        fi
        echo "Source index '$index' exists."
    done
}

check_dest_index_not_exists() {
    for index in "${SOURCE_INDEXES[@]}"; do
        DEST_INDEX="${index}-${DEST_SUFFIX}"
        response=$(curl -s -o /dev/null -w "%{http_code}" "$ES_HOST/$DEST_INDEX")
        if [ "$response" -eq 200 ]; then
            echo "Error: Destination index '$DEST_INDEX' already exists."
            exit 3
        fi
        echo "Destination index '$DEST_INDEX' does not exist."
    done
}

validate_query() {
    if [ -n "$QUERY" ]; then
        response=$(curl -s -o /dev/null -w "%{http_code}" -X GET "$ES_HOST/_validate/query?explain" \
            -H 'Content-Type: application/json' \
            -d "{\"query\": $QUERY}")
        if [ "$response" -ne 200 ]; then
            echo "Error: The provided query is not valid"
            exit 6
        fi
        echo "Query validated successfully."
    fi
}

# From ES https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-reindex.html#docs-reindex-from-multiple-sources
# Indexing multiple sources
validate_query

start_reindex() {
    for index in "${SOURCE_INDEXES[@]}"; do
        DEST_INDEX="${index}-${DEST_SUFFIX}"
        echo "Starting reindex from '$index' to '$DEST_INDEX'..."

        # Construct the reindex body with optional query and max_docs
        reindex_body="{\"source\": {\"index\": \"$index\""

        if [ -n "$QUERY" ]; then
            reindex_body="${reindex_body}, \"query\": $QUERY"
        fi

        reindex_body="${reindex_body}}, \"dest\": {\"index\": \"$DEST_INDEX\", \"op_type\": \"$OP_TYPE\"}"

        if [ -n "$MAX_DOCS" ]; then
            reindex_body="${reindex_body}, \"max_docs\": $MAX_DOCS"
        fi

        reindex_body="${reindex_body}}"

        echo "Reindex body: $reindex_body"

        task_response=$(curl -s -X POST "$ES_HOST/_reindex?slices=auto&wait_for_completion=false" \
        -H 'Content-Type: application/json' -d "$reindex_body")

        task_id=$(echo "$task_response" | grep -o '"task":"[^"]*"' | cut -d':' -f2- | tr -d '"')

        if [ -z "$task_id" ]; then
            echo "Error: Failed to start reindexing task for index '$index'."
            exit 4
        fi

        echo "Reindexing task started for index '$index' with task ID: $task_id"
    done
}

check_es_alive
check_source_index_exists
check_dest_index_not_exists
start_reindex

echo "Reindexing script executed successfully."
exit 0
