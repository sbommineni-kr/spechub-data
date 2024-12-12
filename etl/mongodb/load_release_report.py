# Audit Log
# Author: sudheer bommineni
# Email: sudheer.bommineni@kroger.com
# ID: kon8383
# Date: 20241001
# Description: This script extracts the maximum timestamp from all collections ending with 'changestream' and writes the results to a CSV file.
# Execution: 
# pre - python etl/mongodb/load_release_report.py -c config/datalake.yaml -a '{"output_path":"/Users/sudheer.bomminenikroger.com/Desktop/work/git/spechub-data/data","release_type":"pre"}' -j mongodb -n load_release_report -v dev
# post - python etl/mongodb/load_release_report.py -c config/datalake.yaml -a '{"output_path":"/Users/sudheer.bomminenikroger.com/Desktop/work/git/spechub-data/data","release_type":"post","start_timestamp":"2024-12-11 06:12:02.754120"}' -j mongodb -n load_release_report -v dev

from core.dl_etlbase import DLETLBase
from core.dl_logger import DLLogger
import os
import csv
from bson.timestamp import Timestamp
from datetime import datetime
from datetime import datetime, timezone

class ExtractMaxChangeStreamTimestampsJob(DLETLBase):
    def __init__(self):
        super().__init__()

    def extract_max_timestamps(self, output_path, release_type):
        
        db_name = self.mongo_database
        db = self.mongo.mongo_client[db_name]
        
        collections = db.list_collection_names()
        changestream_collections = [coll for coll in collections if coll.endswith('changestream')]

        dl_log.info(f"Found {len(changestream_collections)} changestream collections: {changestream_collections}")
        
        results = []
        for collection_name in changestream_collections:
            
            collection = db[collection_name]
            
            # Modified pipeline to correctly get the document with max timestamp
            pipeline = [
                {"$match": {"changeStreamEvent.readableTimestamp": {"$exists": True}}},
                {"$addFields": {
                    "effectiveSubCommodityCode": {
                        "$ifNull": ["$documentAfter.subCommodityCode", "$documentBefore.subCommodityCode"]
                    },
                    "effectiveSubCommodityName": {
                        "$ifNull": ["$documentAfter.subCommodityName", "$documentBefore.subCommodityName"]
                    },
                    "effectiveVariations": {
                        "$ifNull": [
                            "$documentAfter.variations.variationAttributes",
                            "$documentBefore.variations.variationAttributes"
                        ]
                    }
                }},
                {"$group": {
                    "_id": {
                        "subCommodityCode": "$effectiveSubCommodityCode",
                        "variations": "$effectiveVariations"
                    },
                    "max_timestamp": {"$max": "$changeStreamEvent.readableTimestamp"},
                    "subCommodityName": {"$first": "$effectiveSubCommodityName"},
                    "lastDoc": {
                        "$first": {
                            "changeStreamEvent": "$changeStreamEvent",
                            "objectId": "$_id",
                            "documentBefore": "$documentBefore",
                            "documentAfter": "$documentAfter"
                        }
                    }
                }},
                {"$match": {
                    "_id.subCommodityCode": {"$ne": None}
                }}
            ]
            result = list(collection.aggregate(pipeline))
            # Print first document structure
            
            dl_log.info(f"Total documents: {len(result)} in collection {collection_name}")

            if len(result) > 0:
                dl_log.info(f"sample doc : {result[0]}")
            

            for doc in result:
                #dl_log.info(f"SubCommodityCode: {doc['_id']}, Max Timestamp: {doc['max_timestamp']}")
            
            

                if not doc:
                    dl_log.warning("Empty document found in results")
                    continue
                
                
                
                # Modified results append section
                results.append({
                    'collection': collection_name,
                    'max_timestamp': doc.get('max_timestamp', ''),
                    'subCommodityCode': doc['_id']['subCommodityCode'],
                    'subCommodityName': doc.get('subCommodityName', ''),
                    'database_name': db_name,
                    'variations': str(doc['_id'].get('variations', '')),    
                    'object_id': str(doc.get('lastDoc', {}).get('objectId', '')),
                    'changeStreamEvent': str(doc.get('lastDoc', {}).get('changeStreamEvent', ''))
                })

        # Ensure the output directory exists
        os.makedirs(output_path, exist_ok=True)
        output_file = os.path.join(output_path, f'{db_name}_max_changestream_timestamps_{self.end_date}_{release_type}.csv')
        
        with open(output_file, 'w', newline='') as csvfile:
            fieldnames = ['collection', 'max_timestamp', 'subCommodityCode', 'subCommodityName', 
                            'database_name', 'variations', 'object_id', 'changeStreamEvent']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            for row in results:
                writer.writerow(row)

        dl_log.info(f"Successfully extracted max timestamps to {output_file}")
        #print the current utc timestamp in the same format as the timestamp in the changestream collection to use it in the next job
        dl_log.info(f"Current UTC timestamp: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S.%f')}")

    def write_results_to_csv(self, results, output_file):
        """Writes results to CSV file"""
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        
        with open(output_file, 'w', newline='') as csvfile:
            fieldnames = ['collection', 'timestamp', 'subCommodityCode', 'subCommodityName', 'variations',
                        'database_name', 'object_id', 'operation_type', 'changes_summary']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            for row in results:
                writer.writerow(row)
        
        dl_log.info(f"Successfully wrote {len(results)} records to {output_file}")
    

    def extract_post_release_changes(self, output_path, start_timestamp):
        db_name = self.mongo_database
        db = self.mongo.mongo_client[db_name]
        
        collections = db.list_collection_names()
        changestream_collections = [coll for coll in collections if coll.endswith('changestream')]

        dl_log.info(f"Found {len(changestream_collections)} changestream collections: {changestream_collections}")
        
        results = []
        for collection_name in changestream_collections:
            collection = db[collection_name]
            
            pipeline = [
                {
                    "$match": {
                        "changeStreamEvent.readableTimestamp": {
                            "$gt": start_timestamp
                        }
                    }
                },
                {
                    "$addFields": {
                        "effectiveSubCommodityCode": {
                            "$ifNull": ["$documentAfter.subCommodityCode", "$documentBefore.subCommodityCode"]
                        },
                        "effectiveSubCommodityName": {
                            "$ifNull": ["$documentAfter.subCommodityName", "$documentBefore.subCommodityName"]
                        }
                    }
                }
            ]

            result = list(collection.aggregate(pipeline))
            dl_log.info(f"Found {len(result)} changes in {collection_name} after {start_timestamp}")

            for doc in result:
                document_changes = self.analyze_document_changes(
                    doc.get('documentBefore', {}),
                    doc.get('documentAfter', {})
                )
                
                results.append({
                    'collection': collection_name,
                    'timestamp': doc.get('changeStreamEvent', {}).get('readableTimestamp', ''),
                    'subCommodityCode': doc.get('effectiveSubCommodityCode'),
                    'subCommodityName': doc.get('effectiveSubCommodityName'),
                    'variations': str(doc.get('documentAfter', {}).get('variations', '') if doc.get('documentAfter') else '') if collection_name == 'subcommodity_dqa_changestream' else 'N/A',
                    'database_name': db_name,
                    'object_id': str(doc.get('_id', '')),
                    'operation_type': doc.get('changeStreamEvent', {}).get('operationType', ''),
                    'changes_summary': document_changes
                })

        output_file = os.path.join(output_path, f'{db_name}_post_release_changes_{self.end_date}.csv')
        self.write_results_to_csv(results, output_file)
        dl_log.info(f"Post-release changes written to {output_file}")

    
    def analyze_document_changes(self, before, after):
        """Analyzes changes between before and after documents"""
        if not before and after:
            return "New document created"
        if before and not after:
            return "Document deleted"
            
        changes = []
        key_sections = ['spec', 'tolerance', 'defectScoringGuide', 'variations']
        
        for section in key_sections:
            before_section = before.get(section, {})
            after_section = after.get(section, {})
            
            if before_section != after_section:
                if isinstance(before_section, dict) and isinstance(after_section, dict):
                    modified_fields = self.get_modified_fields(before_section, after_section)
                    if modified_fields:
                        changes.append(f"{section}: {', '.join(modified_fields)}")
                else:
                    changes.append(f"{section} modified")
                    
        return " | ".join(changes) if changes else "No significant changes"

    def get_modified_fields(self, before_dict, after_dict):
            """Helper function to identify modified fields in a dictionary"""
            modified = []
            all_keys = set(before_dict.keys()) | set(after_dict.keys())
            
            for key in all_keys:
                if key not in before_dict:
                    modified.append(f"Added {key}")
                elif key not in after_dict:
                    modified.append(f"Removed {key}")
                elif before_dict[key] != after_dict[key]:
                    modified.append(f"Modified {key}")
                    
            return modified


if __name__ == '__main__':
    job = ExtractMaxChangeStreamTimestampsJob()
    dl_log = DLLogger(job.job_name)
    output_path = job.job_additional_args.get('output_path')
    release_type = job.job_additional_args.get('release_type')
    start_timestamp = job.job_additional_args.get('start_timestamp', None)
    
    if release_type == 'pre':
        job.extract_max_timestamps(output_path,release_type)
    elif release_type == 'post':
        job.extract_post_release_changes(output_path,start_timestamp)
