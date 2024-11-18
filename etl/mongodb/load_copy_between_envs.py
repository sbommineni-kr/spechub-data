# Audit Log
# Author: Sudheer Bommineni
# Email: sudheer.bommineni@kroger.com
# ID: KON8383
# Date: 2024-11-03
# Description: This script is used to copy data between MongoDB environments.
# Exection : python /Users/sudheer.bomminenikroger.com/Desktop/work/git/spechub-data/etl/mongodb/load_copy_between_envs.py -c config/datalake.yaml -j mongodb -n load_copy_between_envs -a '{"source_env":"test","target_env":"dev","storage_connection":""}'

from core.dl_etlbase import DLETLBase
from core.dl_logger import DLLogger
import os 
import json
import yaml

from bson.json_util import dumps
from pymongo.errors import OperationFailure
from core.dl_adls import DLAzureDataLake


class CopyBetweenEnvs(DLETLBase):
    def __init__(self):
        super().__init__()
    
    def get_yaml_args(self):

        # Get job yaml args
        job_yaml_args = self.get_job_yaml_args()
        
        # Read the arguments from yaml config
        storage_connection = job_yaml_args['arguments']['storage_connection']
        container_name = job_yaml_args['arguments']['container_name'] 
        collection_names = job_yaml_args['arguments']['collection_names']
        
        # Allow override from command line args if provided
        if 'storage_connection' in self.job_additional_args:
            storage_connection = self.job_additional_args['storage_connection']
        if 'container_name' in self.job_additional_args:
            container_name = self.job_additional_args['container_name']
        if 'collection_names' in self.job_additional_args:
            collection_names = self.job_additional_args['collection_names']
        if 'source_env' in self.job_additional_args:
            source_env = self.job_additional_args['source_env']
        if 'target_env' in self.job_additional_args:
            target_env = self.job_additional_args['target_env']

        
        return storage_connection, container_name, collection_names, source_env, target_env
    
    def __copy_data(self, source_env, target_env, collection_name):
        '''
        Copies data from source_env to target_env for a given collection
        '''
        source_db_name = self.mongo_database[source_env]
        source_mongo = self.mongo_clients[source_env]
        source_db = source_mongo.mongo_client[source_db_name]

        target_db_name = self.mongo_database[target_env]
        target_mongo = self.mongo_clients[target_env]
        target_db = target_mongo.mongo_client[target_db_name]

        
        for collection_name in collection_names:
            
            dl_log.info(f"Copying data from {collection_name} in {source_env} to {target_env}")
            
            #get collection
            source_collection = source_db[collection_name]
            target_collection = target_db[collection_name]
                        
            # Get all documents from source collection
            source_documents = list(source_collection.find({}))
            
            if source_documents:
                # Delete all existing documents in target collection
                target_collection.delete_many({})
                
                # Insert all documents from source to target
                try:
                    target_collection.insert_many(source_documents)
                    dl_log.info(f"Successfully copied {len(source_documents)} documents for collection {collection_name}")
                except OperationFailure as e:
                    dl_log.error(f"Failed to copy documents for collection {collection_name}: {str(e)}")
            else:
                dl_log.warning(f"No documents found in source collection {collection_name}")



    
    
    def copy_data(self, storage_connection, container_name, collection_names, source_env, target_env):

        '''
        1. lists the target_env back up files
        2. prompts user to conform if they want to proceed
        3. copies the files to the target_env from source_env for each collection
        '''
        # Create an instance of DLAzureDataLake
        adls = DLAzureDataLake(connection_string=storage_connection, container_name=container_name)

        #list the files based on target_env
        dl_log.info(f"Listing files in {target_env} environment")
        adls.list_files(prefix=target_env)

        #initiate user prompt  to confirm if they want to proceed
        dl_log.info(f"\nPlease make sure you have backed up the {target_env} collections. if not, use this command from bin directory :\n sh run_backup_all_envs.sh -s <storage_connection> -e {target_env}  OR  .\run_backup_all_envs.ps1 -StorageConnection <storage_connection> -Environment {target_env}")
        
        user_input = input(f"\nDo you want to proceed with copying the collections : {collection_names} from {source_env} to {target_env}? (y/n): ")

        if user_input.lower() == 'y':
            #copy the files from source_env to target_env
            self.__copy_data(source_env, target_env, collection_names)             
        else:
            dl_log.info("CANCELLED: Copy operation cancelled.")


if __name__ == '__main__':
    job = CopyBetweenEnvs()
    dl_log = DLLogger(job.job_name)
    
    #fecth args 
    storage_connection, container_name, collection_names, source_env, target_env = job.get_yaml_args()

    # Call copy_data with the configured parameters
    job.copy_data(
        storage_connection=storage_connection,
        container_name=container_name,
        collection_names=collection_names,
        source_env=source_env,
        target_env=target_env
    )

