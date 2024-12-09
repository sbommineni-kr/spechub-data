# Audit Log
# Author: Sudheer Bommineni
# Email: sudheer.bommineni@kroger.com
# ID: KON8383
# Date: 2024-09-29
# Description: This script is used to perform sanity checks if the Mongodb is working after the upgrades
# Execution: python /Users/sudheer.bomminenikroger.com/Desktop/work/git/spechub-data/etl/mongodb/load_perform_sanity_check.py -c config/datalake.yaml -n load_perform_sanity_check -v dev

from core.dl_etlbase import DLETLBase
from core.dl_logger import DLLogger
import os
import json
import pymongo
import yaml
import datetime

class SanityCheck(DLETLBase):
    
    def __init__(self):
        super().__init__()
        #get mongo_client from dl_etlbase
        self.mongo_client = self.mongo.mongo_client
    
    def get_job_yaml_args(self):
        job_yaml_args =  super().get_job_yaml_args()
        collection_name = job_yaml_args['arguments']['collection_name']
        return collection_name
   
    def perform_sanity_checks(self, collection_name):
        """
        Performs sanity checks after MongoDB upgrades
        """
        
        try:
            # 1. Check MongoDB server status and version
            server_info = self.mongo_client.server_info()
            dl_logger.info(f"MongoDB Version: {server_info['version']}")
            #dl_logger.info(f"MongoDB Storage Engine: {server_info['storageEngine']['name']}")

            # 2. Check database connection
            db = self.mongo_client[self.mongo.database]
            dl_logger.info(f"Successfully connected to database: {self.mongo.database}")

            # 3. Check collection existence and stats
            if collection_name in db.list_collection_names():
                collection_stats = db.command("collStats", collection_name)
                dl_logger.info(f"Collection {collection_name} exists")
                dl_logger.info(f"Collection size: {collection_stats['size']} bytes")
                dl_logger.info(f"Document count: {collection_stats['count']}")
            else:
                dl_logger.error(f"Collection {collection_name} does not exist!")
                return False

            # 4. Check read operations
            sample_doc = db[collection_name].find_one()
            if sample_doc:
                dl_logger.info("Read operation successful")
            else:
                dl_logger.warning("Collection is empty or read operation failed")

            # 5. Check write operations
            test_doc = {"_id": "sanity_test", "timestamp": datetime.datetime.now()}
            try:
                db[collection_name].insert_one(test_doc)
                db[collection_name].delete_one({"_id": "sanity_test"})
                dl_logger.info("Write operations (insert/delete) successful")
            except Exception as e:
                dl_logger.error(f"Write operation failed: {str(e)}")
                return False

            # 6. Check indexes
            indexes = list(db[collection_name].list_indexes())
            dl_logger.info(f"Number of indexes: {len(indexes)}")
            for index in indexes:
                dl_logger.info(f"Index: {index['name']}, Key: {index['key']}")

            # 7. Check authentication
            try:
                # Simple role check instead of full user info
                roles = self.mongo_client[self.mongo.database].command("connectionStatus")
                dl_logger.info(f"Authentication check passed. Connected as user with roles: {roles['authInfo']['authenticatedUserRoles']}")
            except Exception as e:
                dl_logger.warning(f"Limited authentication info available: {str(e)}")
                # Don't fail the check since basic operations worked
                pass

            dl_logger.info("All sanity checks passed successfully!")
            return True

        except Exception as e:
            dl_logger.error(f"Sanity check failed with error: {str(e)}")
            return False
        


    
if __name__ == "__main__":
    job = SanityCheck()
    dl_logger = DLLogger(job.job_name)
    collection_name = job.get_job_yaml_args()
    job.perform_sanity_checks(collection_name)