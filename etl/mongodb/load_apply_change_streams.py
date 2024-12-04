# Audit Log
# Author: Sudheer Bommineni
# Email: sudheer.bommineni@kroger.com
# ID: KON8383
# Date: 2024-09-29
# Description: This script is used to apply change streams to a MongoDB database on a given input collection. 
# Execution: python /Users/sudheer.bomminenikroger.com/Desktop/work/git/spechub-data/etl/mongodb/load_apply_change_streams.py -c config/datalake.yaml -a '{"collection_name":"subcommodity_migration"}' -n load_apply_change_streams -v dev
# nohup setup: 
'''
nohup ./run_change_streams_prod.sh > /Users/sudheer.bomminenikroger.com/Desktop/work/prod_output.log 2>&1 &
nohup ./run_change_streams_uat.sh > /Users/sudheer.bomminenikroger.com/Desktop/work/uat_output.log 2>&1 &

output :
(myenv312) (base) ➜  bin git:(feature) ✗ nohup ./run_change_streams_prod.sh > /Users/sudheer.bomminenikroger.com/Desktop/work/prod_output.log 2>&1 &
[1] 5030
(myenv312) (base) ➜  bin git:(feature) ✗ nohup ./run_change_streams_uat.sh > /Users/sudheer.bomminenikroger.com/Desktop/work/uat_output.log 2>&1 &
[2] 5168
(myenv312) (base) ➜  bin git:(feature) ✗ 

use kill to PID if needed
ps aux | grep run_change_streams_prod.sh
ps aux | grep run_change_streams_uat.sh


'''

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
    
    def check_and_enable_pre_images(self, db_name, collection_name):
        """
        Check if pre-image recording is enabled for a collection and enable it if not.
        """
        db = self.mongo_client[db_name]
        try:
            # Check collection options for pre-image settings
            collection_info = db.command("listCollections", filter={"name": collection_name})
            options = collection_info.get("cursor", {}).get("firstBatch", [])[0].get("options", {})
            
            pre_image_settings = options.get("changeStreamPreAndPostImages", {})
            if pre_image_settings.get("enabled", False):
                dl_logger.info(f"Pre-image recording is already enabled for collection '{collection_name}' in database '{db_name}'.")
                return True
            
            # Enable pre-image recording
            dl_logger.info(f"Enabling pre-image recording for collection '{collection_name}' in database '{db_name}'.")
            db.command({
                "collMod": collection_name,
                "changeStreamPreAndPostImages": {"enabled": True}
            })
            dl_logger.info(f"Pre-image recording enabled for collection '{collection_name}' in database '{db_name}'.")
            return True
        except Exception as e:
            dl_logger.info(f"Error checking or enabling pre-images for collection '{collection_name}': {e}")
            return False
    
    def apply_change_streams(self, db_name, collection_name):
        db = self.mongo_client[db_name]
        
        # Convert single collection name to list for consistent processing
        collection_names = [collection_name] if isinstance(collection_name, str) else collection_name
        
        # Create target collections for change streams if they don't exist
        target_collections = {}
        for coll_name in collection_names:

            # Check and enable pre-images if required
            self.check_and_enable_pre_images(db_name, coll_name)

            change_stream_collection = f"{coll_name}_changestream"
            if change_stream_collection not in db.list_collection_names():
                dl_logger.info(f"Creating collection: {change_stream_collection}")
                db.create_collection(change_stream_collection)
            else:
                dl_logger.info(f"Collection {change_stream_collection} already exists.")
            target_collections[str(coll_name)] = db[change_stream_collection]
    
        pipeline = [
            {'$match': {'ns.coll': {'$in': collection_names}}},
            {'$addFields': {
                'clientInfo': {
                    'ip': {
                        '$ifNull': [
                            '$clientConnectionAddress.ip',
                            '$client.host'
                        ]
                    }
                }
            }}
        ]

        change_stream = db.watch(
            pipeline,
            full_document='updateLookup',
            full_document_before_change='whenAvailable'
        )
    
        dl_logger.info(f"Started watching collections: {collection_names}")
    
        try:
            while True:
                for change in change_stream:
                    dl_logger.debug(f"Change event received: {change['operationType']}")
                    dl_logger.debug(f"change object is : {change}")
                
                    changed_document = {
                        'documentBefore': change.get('fullDocumentBeforeChange'),
                        'documentAfter': change.get('fullDocument'),
                        'changeStreamEvent': {
                            'operationType': change['operationType'],
                            'timestamp': change['clusterTime'],
                            'readableTimestamp': change['clusterTime'].as_datetime().strftime('%Y-%m-%d %H:%M:%S UTC'),
                            'documentKey': change['documentKey'],
                            'clientInfo': change.get('clientInfo', {'ip': 'unknown'}),
                            'updateDescription': change.get('updateDescription', {})
                        }
                    }
                
                    source_collection_name = change['ns']['coll']
                    target_collection = target_collections[str(source_collection_name)]

                    # Generate unique ID including milliseconds for better granularity
                    changed_document['_id'] = f"{source_collection_name}_{change['documentKey'].get('_id', 'unknown')}_{change['clusterTime'].time}"

                    try:
                        result = target_collection.insert_one(changed_document)
                        dl_logger.info(f"Inserted document with _id: {result.inserted_id}")
                        dl_logger.info(f"Captured change in {source_collection_name}: {change['operationType']} - SubCommodityCode: {change.get('fullDocument', {}).get('subCommodityCode') or change.get('fullDocumentBeforeChange', {}).get('subCommodityCode', 'N/A')}")
                    except pymongo.errors.DuplicateKeyError:
                        result = target_collection.replace_one(
                            {'_id': changed_document['_id']},
                            changed_document,
                            upsert=True
                        )
                        dl_logger.info(f"Updated document with _id: {changed_document['_id']}")
                    except Exception as e:
                        dl_logger.error(f"Error processing change stream: {str(e)}")
                        dl_logger.error(f"Failed document: {changed_document}")
                    
        except pymongo.errors.PyMongoError as e:
            dl_logger.error(f"MongoDB error: {str(e)}")
            # Resume the change stream
            change_stream = db.watch(
                pipeline,
                full_document='updateLookup',
                full_document_before_change='whenAvailable',
                resume_after=change_stream.resume_token
            )    
    
    
    def run(self):
        # Get the environment and database name from mongo_client
        db_name = self.mongo_database
        env = self.job_common_args.env.lower()
        #collection_name = self.job_additional_args['collection_name']

        #get job yaml args 
        job_yaml_args = self.get_job_yaml_args()
        collection_name = job_yaml_args['arguments']['collection_name']
        
        if 'collection_name' in self.job_additional_args:
            collection_name = self.job_additional_args['collection_name']
        else:
            collection_name = job_yaml_args['arguments']['collection_name']

        #apply change streams to the collection
        self.apply_change_streams(db_name, collection_name)

if __name__ == "__main__":
    job = LoadApplyChangeStreamsJob()
    dl_logger = DLLogger(job.job_name)
    job.run()