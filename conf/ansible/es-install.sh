#!/bin/sh

# Setup virtual environment
make setup_venv

usage() {
    echo "Usage: $0 [OPTIONS]"
    echo "Run Elasticsearch installation playbook"
    echo ""
    echo "Options:"
    echo "  --inventory FILE    Inventory file (default: inventories/production/hosts.yml)"
    echo "  --user USER         Ansible user (default: \$USER)"
    echo "  --help              Show this help message"
}

error_exit() {
    usage
    exit 1
}

inventory="inventories/production/hosts.yml"
user=""

while [ $# -gt 0 ]; do
    case "$1" in
        --inventory)
            inventory="$2"
            shift 2
            ;;
        --user)
            user="$2"
            shift 2
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

[ -z "$user" ] && user="$USER"

set -- playbooks/es-install.yml \
    -i "$inventory"

set -- "$@" -e "ansible_user=$user"

stty -echo
printf "BECOME password for $user: "
read become_pass
stty echo
printf "\n"
set -- "$@" -e "ansible_become=true" -e "ansible_become_password=$become_pass"
unset become_pass

echo "Running installation with options:"
echo "  Ansible User: $user"

ansible-playbook "$@"
