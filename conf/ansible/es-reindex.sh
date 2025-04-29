#!/bin/sh

# Setup virtual environment
make setup_venv

usage() {
    echo "Usage: $0 [OPTIONS]"
    echo "Run Elasticsearch reindexing playbook"
    echo ""
    echo "Options:"
    echo "  --source-index INDEX      Source index name (required)"
    echo "  --dest-index INDEX        Destination index name (required)"
    echo "  --date-from DATE          Start date (YYYY-MM-DD) (required)"
    echo "  --date-to DATE            End date (YYYY-MM-DD) (required)"
    echo "  --inventory FILE          Inventory file (default: inventories/production/hosts.yml)"
    echo "  --batch-size SIZE         Reindex batch size (optional)"
    echo "  --user USER         Ansible user (default: \$USER)"
    echo "  --ask-become-pass   Prompt for become password (default: false)"
    echo "  --help                    Show this help message"
    echo ""
    echo "Note: Requires either:"
    echo "  1) --user with --ask-become-pass"
    echo "  2) SSH key-based authentication configured"
    echo "  3) Passwordless sudo on target hosts"
}

error_exit() {
    usage
    exit 1
}

source_index=""
dest_index=""
reindex_date_from=""
reindex_date_to=""
inventory="inventories/production/hosts.yml"
es_reindex_batch_size=""
user=""
ask_become_pass=false

while [ $# -gt 0 ]; do
    case "$1" in
        --source-index)
            source_index="$2"
            shift 2
            ;;
        --dest-index)
            dest_index="$2"
            shift 2
            ;;
        --date-from)
            reindex_date_from="$2"
            shift 2
            ;;
        --date-to)
            reindex_date_to="$2"
            shift 2
            ;;
        --inventory)
            inventory="$2"
            shift 2
            ;;
        --batch-size)
            es_reindex_batch_size="$2"
            shift 2
            ;;
        --user)
            user="$2"
            shift 2
            ;;
        --ask-become-pass)
            ask_become_pass=true
            shift
            ;;
        --help)
            usage
            exit 0
            ;;
        *)
            echo "Invalid option: $1"
            error_exit
            ;;
    esac
done

if [ -z "$source_index" ] || [ -z "$dest_index" ] || [ -z "$reindex_date_from" ] || [ -z "$reindex_date_to" ]; then
    echo "Error: Missing required parameters"
    error_exit
fi

if [ ! -f "$inventory" ]; then
    echo "Error: Inventory file not found: $inventory"
    exit 1
fi

[ -z "$user" ] && user="$USER"

set -- playbooks/es-reindex.yml \
    -i "$inventory" \
    -e "source_index=$source_index" \
    -e "dest_index=$dest_index" \
    -e "reindex_date_from=$reindex_date_from" \
    -e "reindex_date_to=$reindex_date_to"

[ -n "$es_reindex_batch_size" ] && set -- "$@" -e "es_reindex_batch_size=$es_reindex_batch_size"

set -- "$@" -e "ansible_user=$user"

if [ "$ask_become_pass" = true ]; then
    stty -echo
    printf "BECOME password for $user: "
    read become_pass
    stty echo
    printf "\n"
    set -- "$@" -e "ansible_become=true" -e "ansible_become_password=$become_pass"
    unset become_pass
elif [ -n "$user" ]; then
    # Only enable become if a specific user was requested
    set -- "$@" -e "ansible_become=true"
fi

echo "Running reindexing with parameters:"
echo "  Inventory: $inventory"
echo "  Source Index: $source_index"
echo "  Destination Index: $dest_index"
echo "  Date Range: $reindex_date_from to $reindex_date_to"
[ -n "$es_reindex_batch_size" ] && echo "  Batch Size: $es_reindex_batch_size"
[ -n "$user" ] && echo "  Ansible User: $user"

ansible-playbook "$@"
