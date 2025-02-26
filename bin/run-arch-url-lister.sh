#!/bin/sh

SCRIPT_DIR="$(dirname "$0")"
. "$SCRIPT_DIR/func.sh"

print_help(){
    echo ""
    echo "Usage: $0 <input_file_path> [output_file_path]"
    echo ""
    echo "Description:"
    echo " Writes a list of URLs in a WARC file to a txt file "
    echo ""
    echo "Arguments:"
    echo " <input_file_path>       Path to an indirect file (a file that contains a list of files to process)."
    echo " [output_file_path]      Optional. Path to output file for the URL list."
    echo "                         Default: <PROJECT_DIR>/data/arch-lister/url_list/<WARC_FILE_NAME>.txt"
    echo ""
    echo " Example:"
    echo "  $0 arch-lister/file-1.txt"
    echo "  $0 arch-lister/file-1.txt url-lists.txt"
}

# Handle help flag
if [ "$1" = "-h" ] || [ "$1" = "--help" ]; then
    print_help
    exit 0
fi

# We expect (1) argument when we want to process files and list URLs
if [ $# -lt 1 ]; then
  print_help
  exit 1
fi

# Verify that the input file path exists
if [ ! -f "$1" ]; then
   echo "Error: The input file '$1' does not exist, please check the path and try again"
   exit 1
fi

OUTPUT_PARAM=""
if [ -n "$2" ]; then
    OUTPUT_PARAM="-o $2"
fi

run_python indexer.scripts.arch-lister "@$1" $OUTPUT_PARAM --rabbitmq-url='-'
