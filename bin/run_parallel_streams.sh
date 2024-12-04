#!/bin/zsh

# UAT Stream
(source ~/anaconda3/etc/profile.d/conda.sh && conda activate myenv312 && /Users/sudheer.bomminenikroger.com/Desktop/work/git/spechub-data/bin/run_change_streams_uat.sh >> /Users/sudheer.bomminenikroger.com/Desktop/work/crontab_logs/changestream_uat_cron.log 2>&1) &

# TEST Stream
(source ~/anaconda3/etc/profile.d/conda.sh && conda activate myenv312 && /Users/sudheer.bomminenikroger.com/Desktop/work/git/spechub-data/bin/run_change_streams_test.sh >> /Users/sudheer.bomminenikroger.com/Desktop/work/crontab_logs/changestream_test_cron.log 2>&1) &

# PROD Stream
(source ~/anaconda3/etc/profile.d/conda.sh && conda activate myenv312 && /Users/sudheer.bomminenikroger.com/Desktop/work/git/spechub-data/bin/run_change_streams_prod.sh >> /Users/sudheer.bomminenikroger.com/Desktop/work/crontab_logs/changestream_prod_cron.log 2>&1) &

# Wait for all background processes to complete
wait