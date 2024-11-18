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
            #set deafult value for env
            env = None
            env = self.job_common_args.env.lower() if self.job_common_args.env is not None else env
            self.env = env
            
            if env in ['dev', 'test', 'uat', 'prod']:
                dl_log.info(f"Given env variable is {env} .. returning specific environment mongo clients..")
                
                connection_string = self.dl_config['mongodb'][env]['connection_string']
                
                database = self.dl_config['mongodb'][env]['database']
                self.mongo = DLMongo(connection_string, database)
                self.mongo_database = database
                
            elif env is None:
                dl_log.info(f"Given env variable as {env}, returning all clients..")
                
                connection_string = self.dl_config['mongodb']
                
                #return all env mongo clients
                self.mongo_clients = {}
                self.mongo_database = {}
                for env in ['dev', 'test', 'uat', 'prod']:
                    connection_string = self.dl_config['mongodb'][env]['connection_string']
                    database = self.dl_config['mongodb'][env]['database']
                    self.mongo_clients[env] = DLMongo(connection_string, database)
                    self.mongo_database[env] = database
            else:
                raise ValueError(f"Invalid Environment argument: {env}")


                

        
    
    def write(self, data):
        pass

