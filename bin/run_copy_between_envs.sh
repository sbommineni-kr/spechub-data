#!/bin/bash
#example command : sh run_copy_between_envs.sh -s <storage_connection> -f test -t dev

# Initialize variables
source_env=""
target_env=""
storage_connection=""
# Set common logging prefix
SCRIPT_NAME="[$(basename $0)]"

# Parse command line arguments
while getopts "s:f:t:" opt; do
    case $opt in
        s)
            storage_connection="$OPTARG"
            ;;
        f)
            source_env="$OPTARG"
            ;;
        t)
            target_env="$OPTARG"
            ;;
        \?)
            echo "$SCRIPT_NAME Invalid option: -$OPTARG"
            exit 1
            ;;
    esac
done

# Validate mandatory arguments
if [ -z "$storage_connection" ] || [ -z "$source_env" ] || [ -z "$target_env" ]; then
    echo "$SCRIPT_NAME Error: All arguments are mandatory"
    echo "$SCRIPT_NAME Usage: sh run_copy_between_envs.sh -s <storage_connection> -f <source_env> -t <target_env>"
    exit 1
fi

# Validate environments
valid_environments=("dev" "test" "uat" "prod")
if [[ ! " ${valid_environments[@]} " =~ " $source_env " ]] || [[ ! " ${valid_environments[@]} " =~ " $target_env " ]]; then
    echo "$SCRIPT_NAME Error: Invalid environment. Allowed values are: ${valid_environments[@]}"
    exit 1
fi

# Execute the Python script
echo "$SCRIPT_NAME Copying data from $source_env to $target_env"
if python $(dirname $0)/../etl/mongodb/load_copy_between_envs.py \
    -c $(dirname $0)/../config/datalake.yaml \
    -j mongodb \
    -n load_copy_between_envs \
    -a "{\"source_env\":\"$source_env\",\"target_env\":\"$target_env\",\"storage_connection\":\"$storage_connection\"}"; then
    echo "$SCRIPT_NAME Copy operation completed successfully"
else
    echo "$SCRIPT_NAME Error: Copy operation failed"
    exit 1
fi