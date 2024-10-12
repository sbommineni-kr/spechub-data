# Audit Log
# Author: Sudheer Bommineni
# Email: sudheer.bommineni@kroger.com
# ID: KON8383
# Date: 2024-09-29
# Description: This is the base class for all ETL jobs.
import argparse
from .dl_dateutils import DLDateUtils
from .dl_mongo import DLMongo
from .dl_logger import DLLogger
from .dl_config import DLConfig
import json
import os

dl_log = DLLogger(__name__)

class DLBase:
    def __init__(self, start_date=None, end_date=None, job_name=None, job_type=None):
        self.start_date = start_date
        self.end_date = end_date
        self.job_name = job_name
        self.job_type = job_type
        self.job_common_args = self.add_common_arguments()
        self.dl_config = self.get_dl_config()
    
    def add_common_arguments(self):
        
        parser = argparse.ArgumentParser(description='Data retrieval arguments')
        parser.add_argument('-s', '--start_date', type=str, help='Start date for the data retrieval', default=DLDateUtils.get_previous_date())
        parser.add_argument('-e', '--end_date', type=str, help='End date for the data retrieval', default=DLDateUtils.get_current_date())
        #cwd 
        cwd = os.getcwd()
        dl_log.info(f"Current working directory: {cwd}")
        #one step down to the config folder
        parser.add_argument('-c', '--config_file', type=str, help='Path to the config file', default=f'{cwd}/config/datalake.yaml', required=False)
        parser.add_argument('-n', '--job_name', type=str, help='Name of the job you want to run to (e.g., load_all_collections_backup)', required=True)
        parser.add_argument('-j', '--job_type', type=str, help='Name of the job_type you want to connect to (e.g., "mongodb", "databricks", "snowflake")', default='mongodb', required=False)
        parser.add_argument('-a', '--additional_args', type=json.loads, help='Additional arguments for the job in the form of dictionary', default=None, required=False)
        parser.add_argument('-v', '--env', type=str, help='environment', default='DEV', required=False)
        common_args = parser.parse_args()
        
        #print all common args 
        for arg in vars(common_args):
            dl_log.info(f"{arg}: {getattr(common_args, arg)}")
        
        #override start_date, end_date,job_name and job_type with the ones provided in the command line
        self.start_date = common_args.start_date
        self.end_date = common_args.end_date
        self.job_name = common_args.job_name
        self.job_type = common_args.job_type
        
        #if additional_Args is not None, update the job_args with the additional_Args
        if common_args.additional_args is not None:
            self.job_additional_args = common_args.additional_args
        else:
            self.job_additional_args = {}
        
        return common_args
    
    def get_dl_config(self):
        
        dl_config = DLConfig(self.job_common_args.config_file)
        
        return dl_config.read_config()
    
    
    def get_job_yaml_args(self):

        #based on job_type fetch the job_args
        if self.job_common_args.job_type == 'mongodb':
            dl_log.info(f"Fetching job_args for mongodb")
            job_yaml_args = self.dl_config['mongodb-jobs'][self.job_name]
            return job_yaml_args
        
        elif self.job_common_args.job_type == 'databricks':
            dl_log.info(f"Fetching job_args for databricks")
            job_yaml_args = self.dl_config['databricks-jobs'][self.job_name]
            return job_yaml_args




