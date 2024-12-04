# spechub-data
Repository for spechub dataservices

This project contains the data services for the SpecHub application.

## Project Structure
- core/: Core functionality of the data services
- config/: Configuration files and settings
- data/: Data storage and management
- tests/: Unit and integration tests

## Setup
1. Clone the repository
2. conda create -n myenv312 python=3.12
3. conda activate myenv312
2. Install dependencies: `pip install -r requirements.txt`
3. Run setup: `python setup.py install`

## adhoc execution of all cron jobs (INCLUDINg BACKUP)
source /Users/sudheer.bomminenikroger.com/anaconda3/etc/profile.d/conda.sh && conda activate myenv312 && crontab -l | grep -v '^#' | cut -f 6- -d ' ' | while IFS= read -r cmd; do (eval "${cmd#* && }" &); done

## adhoc execution of only change streams
crontab -l | tail -n 3 | xargs -I {} -P 3 bash -c "{}"
crontab -l | tail -n 3 | xargs -I {} -P 3 zsh -c "{}" 

## Check if change stream is running 
ps aux | grep "run_change_stream"

## Kill all changestream 
pkill -f run_change_stream