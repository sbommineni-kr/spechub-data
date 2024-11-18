# Audit Log
# Author: sudheer bommineni
# Email: sudheer.bommineni@kroger.com
# ID: kon8383
# Date: 20241001
# Description: This script extracts the maximum timestamp from all collections ending with 'changestream' and writes the results to a CSV file.
# Execution: 
# pre - python etl/mongodb/load_release_report.py -c config/datalake.yaml -a '{"output_path":"/Users/sudheer.bomminenikroger.com/Desktop/work/git/spechub-data/data","release_type":"pre"}' -j mongodb -n load_release_report -v dev
# post - python etl/mongodb/load_release_report.py -c config/datalake.yaml -a '{"output_path":"/Users/sudheer.bomminenikroger.com/Desktop/work/git/spechub-data/data","release_type":"post"}' -j mongodb -n load_release_report -v dev

from core.dl_etlbase import DLETLBase
from core.dl_logger import DLLogger
import os
import csv
from bson.timestamp import Timestamp
from datetime import datetime

class ExtractMaxChangeStreamTimestampsJob(DLETLBase):
    def __init__(self):
        super().__init__()

    def extract_max_timestamps(self, output_path, release_type):
        try:
            db_name = self.mongo_database
            db = self.mongo.mongo_client[db_name]
            
            collections = db.list_collection_names()
            changestream_collections = [coll for coll in collections if coll.endswith('changestream')]
            
            results = []
            for collection_name in changestream_collections:
                collection = db[collection_name]
                pipeline = [
                    {"$match": {"changeStreamEvent.timestamp": {"$exists": True}}},
                    {"$group": {
                        "_id": {
                            "subCommodityCode": "$subCommodityCode",
                            "subCommodityName": "$subCommodityName",
                            "variations": "$variations.variationAttributes"
                        },
                        "max_timestamp": {"$max": "$changeStreamEvent.timestamp"},
                        "doc": {"$first": "$$ROOT"}
                    }}
                ]
                result = list(collection.aggregate(pipeline))
                for item in result:
                    max_timestamp = item['max_timestamp']
                    doc = item['doc']
                    results.append({
                        'collection': collection_name,
                        'max_timestamp': max_timestamp.time,
                        'utc_timestamp': datetime.fromtimestamp(max_timestamp.time).strftime('%Y-%m-%d %H:%M:%S'),
                        'subCommodityCode': doc.get('subCommodityCode', ''),
                        'subCommodityName': doc.get('subCommodityName', ''),
                        'database_name': db_name,
                        'variations': str(doc.get('variations.variationAttributes', '')),
                        'object_id': str(doc.get('_id', '')),
                        'changeStreamEvent': str(doc.get('changeStreamEvent', ''))
                    })

            # Ensure the output directory exists
            os.makedirs(output_path, exist_ok=True)
            output_file = os.path.join(output_path, f'{db_name}_max_changestream_timestamps_{self.end_date}_{release_type}.csv')
            
            with open(output_file, 'w', newline='') as csvfile:
                fieldnames = ['collection', 'max_timestamp', 'utc_timestamp', 'subCommodityCode', 'subCommodityName', 'database_name', 'variations', 'object_id', 'changeStreamEvent']
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                for row in results:
                    writer.writerow(row)

            dl_log.info(f"Successfully extracted max timestamps to {output_file}")
        except Exception as e:
            dl_log.error(f"An error occurred while extracting max timestamps: {str(e)}")


if __name__ == '__main__':
    job = ExtractMaxChangeStreamTimestampsJob()
    dl_log = DLLogger(job.job_name)
    output_path = job.job_additional_args.get('output_path')
    release_type = job.job_additional_args.get('release_type')
    job.extract_max_timestamps(output_path,release_type)
