#!/bin/sh

run_base() {
    # Default values
    env=""
    inventory=""
    user=""
    extra_args=""

    usage() {
        echo "Usage: $0 [OPTIONS]"
        echo ""
        echo "Options:"
        echo "  -e, --env ENV           Environment (currently supports only production)"
        echo "  -i, --inventory FILE    Inventory File"
        echo "  -u, --user USER         Ansible user (default: \$USER)"
        echo "  -a, --ansible-args ARGS Additional arguments to pass to ansible-playbook"
        echo "                          (all remaining arguments after this flag)"
        echo "  -h, --help              Show this help message"
    }

    error_exit() {
        usage
        exit 1
    }

    # Parse common arguments
    while [ $# -gt 0 ]; do
        case "$1" in
            -e|--env)
                env="$2"
                shift 2
                ;;
            -i|--inventory)
                inventory="$2"
                shift 2
                ;;
            -u|--user)
                user="$2"
                shift 2
                ;;
            -h|--help)
                usage
                exit 0
                ;;
            -a|--ansible-args)
                shift
                extra_args="$*"
                break
                ;;
            -*)
                echo "Unknown option: $1"
                error_exit
                ;;
            *)
                break
                ;;
        esac
    done

    [ -z "$user" ] && user="$USER"

    # Use env to get inventory if not provided
    if [ -z "$inventory" ]; then
        if [ -z "$env" ]; then
            echo "Error: You must specify either --env or --inventory"
            error_exit
        fi
        inventory="../inventories/$env/hosts.yml"
    fi

    if [ ! -f "$inventory" ]; then
        echo "Error: Inventory file not found: $inventory"
        exit 1
    fi

    # Set target based on env
    case "$env" in
        local) target="localhost" ;;
        *) target="elasticsearch" ;;
    esac

    # Prompt for sudo password
    stty -echo
    printf "BECOME password for %s: " "$user"
    read become_pass
    stty echo
    printf "\n"

    # Compose base args
    base_args="-i $inventory"
    base_args="$base_args -e ansible_user=$user"
    base_args="$base_args -e ansible_become_password=$become_pass"
    [ -n "$env" ] && base_args="$base_args -e env=$env -e target=$target"

    export base_args
    export extra_args
    export inventory
    export user
    export env
    export target

    unset become_pass
}
