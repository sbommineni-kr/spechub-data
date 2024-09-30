# Audit Log
# Author: Sudheer Bommineni
# Email: sudheer.bommineni@kroger.com
# ID: KON8383
# Date: 2024-09-29
# Description: This script is used to backup all collections in a MongoDB database.
# Exection : python /Users/sudheer.bomminenikroger.com/Desktop/work/git/spechub-data/etl/mongodb/load_all_collections_backup.py -c config/datalake.yaml -a '{"path":"/Users/sudheer.bomminenikroger.com/Desktop/work/git/mongodb_collections_backup"}' -j mongodb -n load_all_collections_backup -v prod


from core.dl_etlbase import DLETLBase
from core.dl_logger import DLLogger
import os 
import json
import yaml

from bson.json_util import dumps
from pymongo.errors import OperationFailure

class LoadAllCollectionsBackupJob(DLETLBase):
    def __init__(self):
        super().__init__()

    
    def export_collections_to_json(self, path):
        
        try:
            # Get the environment and database name from mongo_client
            db_name = self.mongo_database

            # Connect to the database
            db = self.mongo.mongo_client[db_name]

            # Get all collection names
            collections = db.list_collection_names()

            #join the db_name with end_date 
            #and create the path
            path  = os.path.join(path, db_name)
            path = os.path.join(path, self.end_date)
            # Create the directory if it doesn't exist
            os.makedirs(path, exist_ok=True)
            

            for collection_name in collections:
                if collection_name.startswith('system.'):
                    continue
                # Get the collection
                collection = db[collection_name]

                # Query all documents in the collection
                cursor = collection.find()

                # Create a JSON file for each collection
                file_path = os.path.join(path, f"{collection_name}.json")
                with open(file_path, 'w') as file:
                    json.dump(json.loads(dumps(cursor)), file, indent=2)

            dl_log.info(f"Successfully exported {db_name} collections to {path}")
        except OperationFailure as e:
            dl_log.error(f"MongoDB operation failed: {str(e)}")
        except Exception as e:
            dl_log.error(f"An error occurred while exporting collections: {str(e)}")


        
if __name__ == '__main__':
    job = LoadAllCollectionsBackupJob()
    dl_log = DLLogger(job.job_name)
    dl_log.info(f"path {job.job_additional_args}")
    path = job.job_additional_args['path']
    job.export_collections_to_json(path)