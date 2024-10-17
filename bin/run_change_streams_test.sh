#!/bin/bash
while true; do
    python /Users/sudheer.bomminenikroger.com/Desktop/work/git/spechub-data/etl/mongodb/load_apply_change_streams.py -c /Users/sudheer.bomminenikroger.com/Desktop/work/git/spechub-data/config/datalake.yaml -a '{"collection_name":"subcommodity_dqa"}' -n load_apply_change_streams -v test
    sleep 5
done