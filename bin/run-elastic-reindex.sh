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
# Modify the variables ES_HOST, SOURCE_INDEX, and DEST_INDEX as needed.
# Run this script using: sh reindex.sh
# ./run-elastic-reindex.sh ES_HOST SOURCE_INDEX DEST_INDEX
#

# Example:
# ./run-elastic-reindex.sh mc-search-00001 mc-search-v2-00001

# Default values
ES_HOST=${1:-"http://localhost:9200"}
SOURCE_INDEX=$2
DEST_INDEX=$3
OP_TYPE="create"  # Operation type for reindex, could be `create` or `index`

if [ -z "$SOURCE_INDEX" ] || [ -z "$DEST_INDEX" ]; then
    echo "Error: Both SOURCE_INDEX and DEST_INDEX are required."
    echo "Usage: sh run-elastic-reindex.sh [ES_HOST] SOURCE_INDEX DEST_INDEX"
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
    response=$(curl -s -o /dev/null -w "%{http_code}" "$ES_HOST/$SOURCE_INDEX")
    if [ "$response" -ne 200 ]; then
        echo "Error: Source index '$SOURCE_INDEX' does not exist."
        exit 2
    fi
    echo "Source index '$SOURCE_INDEX' exists."
}

check_dest_index_not_exists() {
    response=$(curl -s -o /dev/null -w "%{http_code}" "$ES_HOST/$DEST_INDEX")
    if [ "$response" -eq 200 ]; then
        echo "Error: Destination index '$DEST_INDEX' already exists."
        exit 3
    fi
    echo "Destination index '$DEST_INDEX' does not exist."
}

start_reindex() {
    echo "Starting reindex from '$SOURCE_INDEX' to '$DEST_INDEX'..."
    task_response=$(curl -s -X POST "$ES_HOST/_reindex?wait_for_completion=false" \
    -H 'Content-Type: application/json' -d "
    {
      \"source\": { \"index\": \"$SOURCE_INDEX\" },
      \"dest\": { \"index\": \"$DEST_INDEX\", \"op_type\": \"$OP_TYPE\" }
    }")

    task_id=$(echo "$task_response" | grep -o '"task":"[^"]*"' | cut -d':' -f2 | tr -d '"')

    if [ -z "$task_id" ]; then
        echo "Error: Failed to start reindexing task."
        exit 4
    fi

    echo "Reindexing task started with task ID: $task_id"
}

# Main script execution
check_es_alive
check_source_index_exists
check_dest_index_not_exists
start_reindex

echo "Reindexing script executed successfully."
exit 0
