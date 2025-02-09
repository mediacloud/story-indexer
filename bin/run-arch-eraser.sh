#!/bin/sh

SCRIPT_DIR="$(dirname "$0")"
. "$SCRIPT_DIR/func.sh"

print_help(){
    echo ""
    echo "Usage: $0 <path_to_url_list_file> [OPTIONS]"
    echo ""
    echo "Description:"
    echo " Description: Deletes documents from Elasticsearch based on original URLs provided in input files"
    echo ""
    echo "Options:"
    echo "  --elasticsearch-hosts    Elasticsearch host URL"
    echo "  --indices                The name of the Elasticsearch indices to delete from"
    echo "  --min-delay              The minimum time to wait between delete operations (default: 0.5 seconds)"
    echo "  --max-delay              The maximum time to wait between delete operations (default: 3.0 seconds)"
    echo "  --fetch-batch-size       The number of documents to fetch from Elasticsearch in each batch (default: 1000)"
    echo "  --batch-delete           Enable batch deletion of documents (default: False)"
    echo "  --buffer                 The maximum number of delete operations to buffer before flushing to Elasticsearch (default: 2000)"
    echo ""
    echo " Example:"
    echo "  $0  arch-lister/url_list --elasticsearch-hosts=http://localhost:9200 --indices=index1,index2 --fetch-batch-size=5000 --min-delay=1 --max-delay=3"
    echo "  $0  arch-lister/url_list --elasticsearch-hosts=http://localhost:9200 --indices=index1,index2 --fetch-batch-size=5000 --min-delay=1 --max-delay=3 --batch-delete --buffer=1000"
}

# Handle help flag
if [ "$1" = "-h" ] || [ "$1" = "--help" ]; then
    print_help
    exit 0
fi

if [ $# -lt 1 ]; then
  print_help
  exit 1
fi

input_path=$1
shift 1

run_python indexer.scripts.arch-eraser "$input_path" "$@" --rabbitmq-url='-'
