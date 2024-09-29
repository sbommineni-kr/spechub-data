# Audit Log
# Author: Sudheer Bommineni
# Email: sudheer.bommineni@kroger.com
# ID: KON8383
# Date: 2024-09-29
# Description: This is the base class for all ETL jobs.


from .dl_base import DLBase
from .dl_logger import DLLogger
from .dl_mongo import DLMongo

dl_log = DLLogger(__name__)

class DLETLBase(DLBase):
    def __init__(self, start_date=None, end_date=None, job_name=None, job_type=None):
        
        super().__init__(start_date, end_date, job_name, job_type)

        if self.job_type == 'mongodb':
            # Initialize MongoDB connection
            env = self.job_common_args.env.lower()
            
            if env not in ['dev', 'test', 'uat', 'prod']:
                raise ValueError(f"Invalid environment: {env}")
            else:
                connection_string = self.dl_config['mongodb'][env]['connection_string']
                database = self.dl_config['mongodb'][env]['database']
                self.mongo = DLMongo(connection_string, database)
                self.mongo_database = database

        
    
    def write(self, data):
        pass

