#!/bin/sh

# Setup virtual environment
make setup_venv

usage() {
    echo "Usage: $0 [OPTIONS]"
    echo "Run Elasticsearch configuration playbook"
    echo ""
    echo "Options:"
    echo "  --ilm-only          Configure only ILM policies"
    echo "  --template-only     Configure only index templates"
    echo "  --index-only        Configure only initial index"
    echo "  --inventory FILE    Inventory file (default: inventories/production/hosts.yml)"
    echo "  --user USER         Ansible user (default: current user)"
    echo "  --ask-become-pass   Prompt for become password"
    echo "  --verbose           Enable verbose output"
    echo "  --help              Show this help message"
}

error_exit() {
    usage
    exit 1
}

tags=""
inventory="inventories/production/hosts.yml"
verbose=""
user=""
ask_become_pass=false

while [ $# -gt 0 ]; do
    case "$1" in
        --ilm-only)
            tags="es_ilm"
            shift
            ;;
        --template-only)
            tags="es_template"
            shift
            ;;
        --index-only)
            tags="es_index"
            shift
            ;;
        --inventory)
            inventory="$2"
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
        --verbose)
            verbose="-v"
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

if [ ! -f "$inventory" ]; then
    echo "Error: Inventory file not found: $inventory"
    exit 1
fi

set -- playbooks/es-configure.yml \
    -i "$inventory"

[ -n "$tags" ] && set -- "$@" --tags "$tags"
[ -n "$user" ] && set -- "$@" -e "ansible_user=$user"
[ -n "$verbose" ] && set -- "$@" "$verbose"

if [ "$ask_become_pass" = true ]; then
    stty -echo
    printf "BECOME password: "
    read become_pass
    stty echo
    printf "\n"
    set -- "$@" -e "ansible_become_password=$become_pass"
    unset become_pass
fi

echo "Running with options:"
[ -n "$tags" ] && echo "  Tags: $tags"
echo "  Inventory: $inventory"
[ -n "$user" ] && echo "  Ansible User: $user"
[ -n "$verbose" ] && echo "  Verbose mode enabled"

ansible-playbook "$@"
