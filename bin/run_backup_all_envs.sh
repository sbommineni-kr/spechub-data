#!/bin/bash
#execution command for all environments: sh run_backup_all_envs.sh -s <storage_connection>
#execution command for a specific environment: sh run_backup_all_envs.sh -s <storage_connection> -e dev

#cronjob is setup .. , execute crontab -l to see the cronjob , scheduled to run at 4:00 PM CST daily
environments=("dev" "test" "uat" "prod")
storage_connection=""
env_arg=""

SCRIPT_NAME="[$(date '+%Y-%m-%d %H:%M:%S')] [$(basename $0)]"
# Parse command line arguments
while getopts "s:e:" opt; do
    case $opt in
        s)
            storage_connection="$OPTARG"
            ;;
        e)
            env_arg="$OPTARG"
            ;;
        \?)
            echo "$SCRIPT_NAME Invalid option: -$OPTARG"
            exit 1
            ;;
    esac
done

# Check if storage_connection is provided
if [ -z "$storage_connection" ]; then
    echo "$SCRIPT_NAME Error: Storage connection string is mandatory. Use -s option."
    exit 1
fi

# Check if environment argument is provided and valid
if [ ! -z "$env_arg" ]; then
    if [[ " ${environments[@]} " =~ " $env_arg " ]]; then
        environments=("$env_arg")
    else
        echo "$SCRIPT_NAME Error: Invalid environment. Allowed values are: ${environments[@]}"
        exit 1
    fi
fi

for env in "${environments[@]}"
do
    echo "$SCRIPT_NAME Running backup for environment: $env"
    echo "$SCRIPT_NAME Directory: $(dirname $0)"
    if python $(dirname $0)/../etl/mongodb/load_spechub_backup_adls.py \
        -c $(dirname $0)/../config/datalake.yaml \
        -j mongodb \
        -n load_spechub_backup_adls \
        -a "{\"storage_connection\":\"$storage_connection\"}" \
        -v "$env"; then
        echo "$SCRIPT_NAME Successfully completed backup for environment: $env"
    else
        echo "$SCRIPT_NAME Error: Backup failed for environment: $env"
        exit 1
    fi
done