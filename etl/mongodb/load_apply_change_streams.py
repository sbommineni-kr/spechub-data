# Audit Log
# Author: Sudheer Bommineni
# Email: sudheer.bommineni@kroger.com
# ID: KON8383
# Date: 2024-09-29
# Description: This script is used to apply change streams to a MongoDB database on a given input collection. 
# Execution: python /Users/sudheer.bomminenikroger.com/Desktop/work/git/spechub-data/etl/mongodb/load_apply_change_streams.py -c config/datalake.yaml -a '{"collection_name":"subcommodity_migration"}' -n load_apply_change_streams -v dev

from core.dl_etlbase import DLETLBase
from core.dl_logger import DLLogger
import os
import json
import pymongo
import yaml

class LoadApplyChangeStreamsJob(DLETLBase):
    
    def __init__(self):
        super().__init__()
        #get mongo_client from dl_etlbase
        self.mongo_client = self.mongo.mongo_client
    
    def apply_change_streams(self, db_name, collection_name):
        # Connect to the database
        db = self.mongo_client[db_name]
        # Get the source collection
        source_collection = db[collection_name]
        # Get the target collection name
        target_collection_name = collection_name + '_changestream'
        
        # Create collection if it doesn't exist
        target_collection = self.mongo.create_collection(target_collection_name)

        # Get the change stream
        change_stream = source_collection.watch(full_document='updateLookup')
        
        # Iterate over the change stream
        for change in change_stream:
            # Extract the changed document
            if change['operationType'] in ['insert', 'update', 'replace']:
                changed_document = change.get('fullDocument', {})
            elif change['operationType'] == 'delete':
                changed_document = change['documentKey']
            elif change['operationType'] in ['drop', 'dropDatabase', 'rename', 'invalidate']:
                changed_document = change
            else:
                continue  # Skip other operation types
            
            # Add metadata about the change
            changed_document['changeStreamEvent'] = {
                'operationType': change['operationType'],
                'timestamp': change['clusterTime']
            }
            
            # Generate a unique _id for the change stream document
            changed_document['_id'] = f"{changed_document.get('_id', 'unknown')}_{change['clusterTime'].time}"
            
            # Insert the changed document into the target collection
            try:
                target_collection.insert_one(changed_document)
            except pymongo.errors.DuplicateKeyError:
                # If a duplicate key error occurs, update the existing document
                target_collection.replace_one(
                    {'_id': changed_document['_id']},
                    changed_document,
                    upsert=True
                )
            except Exception as e:
                # Log any other errors
                self.logger.error(f"Error processing change stream: {str(e)}")
        
    
    def run(self):
        # Get the environment and database name from mongo_client
        db_name = self.mongo_database
        env = self.job_common_args.env.lower()
        collection_name = self.job_additional_args['collection_name']

        #apply change streams to the collection
        self.apply_change_streams(db_name, collection_name)

if __name__ == "__main__":
    job = LoadApplyChangeStreamsJob()
    dl_logger = DLLogger(job.job_name)
    job.run()