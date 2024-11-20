
# Audit Log
# Author: Sudheer Bommineni
# Email: sudheer.bommineni@kroger.com
# ID: KON8383
# Date: 2024-11-03
# Description: This script is used to copy data from prod to non-prod MongoDB database.
# Exection : python /Users/sudheer.bomminenikroger.com/Desktop/work/git/spechub-data/etl/mongodb/load_spechub_backup_adls.py -c config/datalake.yaml -j mongodb -n load_spechub_backup_adls -a '{"storage_connection":""}' -v prod

#chmod +x /Users/sudheer.bomminenikroger.com/Desktop/work/git/spechub-data/bin/run_backup_all_envs.sh
#export PYTHONPATH="/Users/sudheer.bomminenikroger.com/Desktop/work/git/spechub-data:$PYTHONPATH"


from core.dl_etlbase import DLETLBase
from core.dl_logger import DLLogger
import os 
import json
import yaml

from bson.json_util import dumps
from pymongo.errors import OperationFailure
from core.dl_adls import DLAzureDataLake


class BackupSpecHubData(DLETLBase):
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
        
        return storage_connection, container_name, collection_names

    
    def copy_data_to_adls(self, container_name, collection_names, connection_string):

        try:
            # Get the environment and database name from mongo_client
            db_name = self.mongo_database

            # Connect to the database
            db = self.mongo.mongo_client[db_name]

            # Get collection names
            
            for collection_name in collection_names:

                dl_log.info(f"Copying data from {collection_name} to {container_name}")

                # Get the collection
                collection = db[collection_name]
                
                # Query all documents from the collection
                cursor = collection.find({})
                
                # Convert cursor to list of documents and dump to JSON
                documents = dumps(list(cursor))
                
                # Create blob name with collection name
                blob_name = f"{self.env}/{self.end_date}/{collection_name}.json"
                
                # Upload to blob storage using DLAzureDataLake
                adls = DLAzureDataLake(connection_string=connection_string, container_name=container_name)
                
                # Create a temporary JSON file
                dl_log.info(f"current working directory: {os.getcwd()} ")
                
                temp_file = f"data/{blob_name}"

                #create a temp_file directory if not exists
                temp_file_dir = os.path.dirname(temp_file)
                dl_log.info(f"temp_file_dir: {temp_file_dir}")
                
                if not os.path.exists(temp_file_dir):
                    os.makedirs(temp_file_dir)
                
                with open(temp_file, "w") as f:
                    f.write(documents)
                
                # Upload the JSON file to ADLS
                adls.upload_file(temp_file, blob_name)
                
                # Clean up temporary file
                os.remove(temp_file) 

                #clean up temp_file_dir until env 
                os.removedirs(temp_file_dir)               
                
                dl_log.info(f"Successfully exported collection {collection_name} to blob storage")
            
        except OperationFailure as e:
            dl_log.error(f"MongoDB operation failed: {str(e)}")
        except Exception as e:
            dl_log.error(f"An error occurred while exporting collections: {str(e)}")
        

if __name__ == '__main__':
    job = BackupSpecHubData()
    dl_log = DLLogger(job.job_name)
    
    #fecth args 
    storage_connection, container_name, collection_names = job.get_yaml_args()

    # Call copy_data with the configured parameters
    job.copy_data_to_adls(
        container_name=container_name,
        collection_names=collection_names,
        connection_string=storage_connection
    )


