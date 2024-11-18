# Audit Log
# Author: Sudheer Bommineni
# Email: sudheer.bommineni@kroger.com
# ID: KON8383
# Date: 2024-09-29
# Description: This script is used to load and convert all documents from a MongoDB collection.
# Execution: python /Users/sudheer.bomminenikroger.com/Desktop/work/git/spechub-data/etl/mongodb/load_convert_all_docs.py -c config/datalake.yaml -a '{"collection_name":"subcommodity_migration"}' -n load_convert_all_docs -v test

from core.dl_etlbase import DLETLBase
from core.dl_logger import DLLogger
import os
import json
import pymongo
import yaml

dl_log = DLLogger('load_convert_all_docs')

class LoadConvertAllDocsJob(DLETLBase):

    def __init__(self):
        super().__init__()
        #get mongo_client from dl_etlbase
        self.mongo_client = self.mongo.mongo_client

    def load_convert_all_docs(self, db_name, collection_name):
        # Connect to the database
        db = self.mongo_client[db_name]
        # Get the source collection
        source_collection = db[collection_name]
        # Get the target collection name
        target_collection_name = collection_name + '_dqa_test'

        # Create collection if it doesn't exist
        target_collection = self.mongo.create_collection(target_collection_name)

        # Get all documents from the source collection
        source_docs = source_collection.find()

        # Convert and load each document into the target collection
        all_docs = []
        for doc in source_docs:
            #dl_log.info(f"Converting document: {doc['subCommodityCode']}")
            if len(doc.get("subCommodityCode")) == 5:
                dl_log.info(f"Converting document: {doc['subCommodityCode']}")
                converted_doc = self.convert_document(doc)
                #target_collection.insert_one(converted_doc)
                all_docs.append(converted_doc)
        
                #Write all docs to a file
                output_path = '/Users/sudheer.bomminenikroger.com/Desktop/work/git/spechub-data/data'
                output_file = os.path.join(output_path, f"{collection_name}_{db_name}_all_docs.json")
                with open(output_file, 'w') as f:
                    json.dump(all_docs, f, indent=4)
    
    def delete_tolerance(self, converted_doc):

        #if tolerance is empty then remove that key 
        if converted_doc["tolerance"] == {}:
            del converted_doc["tolerance"]
        
        if 'usdaArrivalTolerance' in converted_doc['tolerance']:
            
            if 'damageTolerance' in converted_doc['tolerance']['usdaArrivalTolerance']:
                
                if 'list' in converted_doc['tolerance']['usdaArrivalTolerance']['damageTolerance']:
                    
                    if converted_doc['tolerance']['usdaArrivalTolerance']['damageTolerance']['list'] == {}:
                        del converted_doc['tolerance']['usdaArrivalTolerance']['damageTolerance']
            
            if 'qualityTolerance' in converted_doc['tolerance']['usdaArrivalTolerance']:
                
                if 'list' in converted_doc['tolerance']['usdaArrivalTolerance']['qualityTolerance']:
                    
                    if converted_doc['tolerance']['usdaArrivalTolerance']['qualityTolerance']['list'] == {}:
                        
                        del converted_doc['tolerance']['usdaArrivalTolerance']['qualityTolerance']
            
            if converted_doc['tolerance']['usdaArrivalTolerance'] == {}:
                del converted_doc['tolerance']
            
    def delete_defectscoring_guide(self, converted_doc):
        #if defectScoringGuide is empty then remove that key
        if converted_doc["defectScoringGuide"] == {}:
            del converted_doc["defectScoringGuide"]
        
        else:
            if converted_doc["defectScoringGuide"]['list'] == []:
                del converted_doc["defectScoringGuide"]
    
    def delete_variations(self, converted_doc):
        #if variations is None, delete varaitions 
        if converted_doc['variations'] is None:
            del converted_doc['variations']


    def default_unitofmeasure(self,converted_doc):

        unitofmeasure = {
            "unitOfMeasure": {
                "pullUom": {
                    "pullType": {
                        
                        "dqaHelperChecked": False,
                        "dqaHelperText": "",
                        "value": "Count",
                        "displayFlag": True
                    },
                    "uomToPull": {
                        
                        "dqaHelperChecked": False,
                        "dqaHelperText": "",
                        "value": "",
                        "displayFlag": True
                    }
                },
                "additionalDescriptor": {
                
                    "dqaHelperChecked": False,
                    "dqaHelperText": "",
                    "displayFlag": True,
                    "value": ""
                },
                "inspectionUom": {
                    "inspectionType": {
                        
                        "dqaHelperChecked": False,
                        "dqaHelperText": "",
                        "value": "",
                        "displayFlag": True
                    },
                    "uomOfInspection": {
                        
                        "dqaHelperChecked": False,
                        "dqaHelperText": "",
                        "value": "",
                        "displayFlag": True
                    }
                }
            }
        }

        converted_doc['spec'].update(unitofmeasure)
    
    def default_smartsamples(self, converted_doc):

        smartsamples = {
            "smartSamples": {
                "positiveTrend": {
                    "inspectionDetailLevel": {
                    
                    "dqaHelperText": "",
                    "dqaHelperChecked": False,
                    "displayFlag": True,
                    "value": ""
                    },
                    "minimumSampleQuantity": {
                    
                    "dqaHelperText": "",
                    "dqaHelperChecked": False,
                    "displayFlag": True,
                    "value": ""
                    },
                    "incrementAmount": {
                    
                    "dqaHelperText": "",
                    "dqaHelperChecked": False,
                    "displayFlag": True,
                    "value": ""
                    },
                    "maximumSampleQuantity": {
                    
                    "dqaHelperText": "",
                    "dqaHelperChecked": False,
                    "displayFlag": True,
                    "value": ""
                    },
                    "threshold": {
                    
                    "dqaHelperText": "",
                    "dqaHelperChecked": False,
                    "displayFlag": True,
                    "value": ""
                    }
                },
                "negativeTrend": {
                    "inspectionDetailLevel": {
                    
                    "dqaHelperText": "",
                    "dqaHelperChecked": False,
                    "displayFlag": True,
                    "value": "Dynamic"
                    },
                    "minimumSampleQuantity": {
                    
                    "dqaHelperText": "",
                    "dqaHelperChecked": False,
                    "displayFlag": True,
                    "value": ""
                    },
                    "incrementAmount": {
                    
                    "dqaHelperText": "",
                    "dqaHelperChecked": False,
                    "displayFlag": True,
                    "value": ""
                    },
                    "maximumSampleQuantity": {
                    
                    "dqaHelperText": "",
                    "dqaHelperChecked": False,
                    "displayFlag": True,
                    "value": ""
                    },
                    "threshold": {
                    
                    "dqaHelperText": "",
                    "dqaHelperChecked": False,
                    "displayFlag": True,
                    "value": ""
                    }
                }
                }
        }

        converted_doc['spec'].update(smartsamples)


    def convert_document(self, doc):
        converted_doc = {
            "subCommodityCode": doc.get("subCommodityCode", None),
            "subCommodityName": doc.get("subCommodityName", None),
            "commodityCode": doc.get("commodityCode", None),
            "defaultFlag": doc.get("defaultFlag", None),
            "effectiveStartDate": doc.get("effectiveStartDate", None),
            "effectiveEndDate": doc.get("effectiveEndDate", None),
            "fullSpec": doc.get("fullSpec", None),
            "status": doc.get("status", None),
            "configVersion": doc.get("configVersion", None),
            "specVersion": doc.get("specVersion", None),
            "lastUpdatedUser": doc.get("lastUpdatedUser", None),
            "lastUpdatedUTCTimestamp": doc.get("lastUpdatedUTCTimestamp", None),
            "disableDQA": doc.get("disableDQA", None),
            "subCommodityIcon": doc.get("subCommodityIcon", None),
            "variations": doc.get("variations", None)
        }

        # Convert 'spec' section
        converted_doc["spec"] = self.convert_nested_section(doc.get("spec", {}))

        #update "spec" with default unitOfMeasure and smartSamples fields
        self.default_unitofmeasure(converted_doc)
        self.default_smartsamples(converted_doc)
        

        # Replace the existing 'tolerance' conversion with this new function
        converted_doc["tolerance"] = self.convert_tolerance(doc.get("tolerance", {}))
        
        # Convert 'defectScoringGuide' section
        converted_doc["defectScoringGuide"] = self.convert_defect_scoring_guide(doc.get("defectScoringGuide", {}))

        #delete invalid tolerance
        self.delete_tolerance(converted_doc)

        #delete invalid defectscoringguide
        self.delete_defectscoring_guide(converted_doc)
        
        #delete variations 
        self.delete_variations(converted_doc)

        return converted_doc


    def convert_nested_section(self, section):
        converted_section = {}
        for key, value in section.items():
            parts = key.split('_')
            current = converted_section
            for part in parts[:-1]:
                if part not in current:
                    current[part] = {}
                current = current[part]
            
            if isinstance(value, dict):
                current[parts[-1]] = {
                    "value": value.get("value"),
                    "displayFlag": value.get("displayFlag", True),
                    "dqaHelperChecked": value.get("dqaHelperChecked", False),
                    "dqaHelperText": value.get("dqaHelperText", "")
                }
            else:
                current[parts[-1]] = value
        
        return converted_section

    def convert_tolerance_section(self, tolerance_data):
        
        '''
        Modify this type of tolerance section 
        -------------------------------------------------------------
        "tolerance": {
            "usdaArrivalTolerance_damageInspection": {
            "displayFlag": True,
            "list": [
                {
                "toleranceName": "Damage",
                "value": "10",
                "uom": "%",
                "dqaHelperText": "",
                "dqaHelperChecked": False,
                "displayFlag": True,
                "passOrFailCheckBox": True
                },
                {
                "toleranceName": "Serious Damage",
                "value": "5",
                "uom": "%",
                "dqaHelperText": "",
                "dqaHelperChecked": False,
                "displayFlag": True,
                "passOrFailCheckBox": True
                },
                {
                "toleranceName": "Decay",
                "value": "2",
                "uom": "%",
                "dqaHelperText": "",
                "dqaHelperChecked": False,
                "displayFlag": True,
                "passOrFailCheckBox": True
                }
            ]
            },
            "usdaArrivalTolerance_qualityInspection": {
            "displayFlag": True,
            "list": [
                {
                "toleranceName": "Underweight",
                "value": "0",
                "uom": "%",
                "dqaHelperText": "",
                "dqaHelperChecked": False,
                "displayFlag": True,
                "passOrFailCheckBox": True
                }
            ]
            }
        }
        -------------------------to below-------------------------------

        "tolerance": {
      "usdaArrivalTolerance": {
        "damageTolerance": {
          "dqaHelperChecked": False,
          "dqaHelperText": "",
          "list": {
            "damage": {
              "value": 10,
              "displayFlag": True,
              "dqaHelperChecked": False,
              "dqaHelperText": "",
              "unitOfMeasure": "",
              "passOrFailCheckBox": True
            },
            "seriousDamage": {
              "value": 5,
              "displayFlag": True,
              "dqaHelperChecked": False,
              "dqaHelperText": "",
              "unitOfMeasure": "",
              "passOrFailCheckBox": True
            },
            "decay": {
              "value": 2,
              "displayFlag": True,
              "dqaHelperChecked": False,
              "dqaHelperText": "",
              "unitOfMeasure": "",
              "passOrFailCheckBox": True
            }
          }
        },
        "qualityTolerance": {
          "dqaHelperChecked": False,
          "dqaHelperText": "",
          "list": {
            "underweight": {
              "value": "0",
              "displayFlag": True,
              "dqaHelperChecked": False,
              "dqaHelperText": "",
              "unitOfMeasure": "",
              "passOrFailCheckBox": True
            }
          }
        }
      }
    }

        '''


        converted_tolerance = {}

        #handle 
    
        
    
        return converted_tolerance

    @staticmethod
    def to_camel_case(snake_str):
        # Converts string with spaces to camelCase
        components = snake_str.split(' ')
        return components[0].lower() + ''.join(x.title() for x in components[1:])
    
    def convert_tolerance(self,data):
        # Create a new structure for the transformed object
        transformed = {
                "usdaArrivalTolerance": {
                    "damageTolerance": {
                        "dqaHelperChecked": False,
                        "dqaHelperText": "",
                        "list": {}
                    },
                    "qualityTolerance": {
                        "dqaHelperChecked": False,
                        "dqaHelperText": "",
                        "list": {}
                    }
                }
        }

        if data != {} or data != None:
        
            # Handle the damage inspection list
            #dl_log.info(f"data: {data}")
            if "usdaArrivalTolerance_damageInspection" in data:
                #if the "list" is null , pop the damageTolerance from the transformed object
                if data["usdaArrivalTolerance_damageInspection"]["list"]:

                
                    for item in data["usdaArrivalTolerance_damageInspection"]["list"]:
                        
                        tolerance_name = self.to_camel_case(item["toleranceName"])
                        
                        transformed["usdaArrivalTolerance"]["damageTolerance"]["list"][tolerance_name] = {
                            "value": item.get("value", '0').replace("%", ""),
                            "displayFlag": item.get("displayFlag", False),
                            "dqaHelperChecked": item.get("dqaHelperChecked", False),
                            "dqaHelperText": item.get("dqaHelperText", ""),
                            "unitOfMeasure": "",
                            "passOrFailCheckBox": item.get("passOrFailCheckBox", False)
                        }
                else:
                    transformed["usdaArrivalTolerance"].pop("damageTolerance")
            
            # Handle the quality inspection list
            if "usdaArrivalTolerance_qualityInspection" in data:
                #if the "list" is null , pop the qualityTolerance from the transformed object
                if data["usdaArrivalTolerance_qualityInspection"]["list"]:
                    for item in data["usdaArrivalTolerance_qualityInspection"]["list"]:
                        
                        tolerance_name = self.to_camel_case(item["toleranceName"])
                        
                        transformed["usdaArrivalTolerance"]["qualityTolerance"]["list"][tolerance_name] = {
                            "value": item.get("value", '0').replace("%", ""),
                            "displayFlag": item.get("displayFlag", False),
                            "dqaHelperChecked": item.get("dqaHelperChecked", False),
                            "dqaHelperText": item.get("dqaHelperText", ""),
                            "unitOfMeasure": "",
                            "passOrFailCheckBox": item.get("passOrFailCheckBox", False)
                        }
                else:
                    transformed["usdaArrivalTolerance"].pop("qualityTolerance")
        else:
            dl_log.info("Data is empty or None")
            transformed = {}

        
        return transformed


    def convert_defect_scoring_guide(self, defect_scoring_guide):
        if defect_scoring_guide:

            #dl_log.info(f"defectscoringguide : {defect_scoring_guide}")

            converted_guide = {
                "displayFlag": defect_scoring_guide.get("displayFlag", False),
                "list": []
            }

            def parse_defectdescription(defectDescription):
                if defectDescription:
                    defects = {}
                    for defect in defectDescription:
                        defect_type = self.to_camel_case(defect["defectType"])
                        value = defect.get('defectDesc', '')

                        defects[defect_type] = {"value": value}
                else:
                    defects = {}
                return defects

            #iterate over each item in list if not empty 
            if defect_scoring_guide["list"]:
                #dl_log.info(f"defect_scoring_guide['list']: {defect_scoring_guide['list']}")
                for item in defect_scoring_guide["list"]:
                    #dl_log.info(f"item: {item}")
                    
                    if 'defectDescription' in item:
                        
                        converted_guide["list"].append({
                            "value": item.get("defectName"),
                            "displayFlag": item.get("displayFlag", False),
                            "dqaHelperChecked": item.get("dqaHelperChecked", False),
                            "dqaHelperText": item.get("dqaHelperText", ""),
                            **parse_defectdescription(item['defectDescription'])
                        })

                        

                        #dl_log.info(f"converted_guide: {converted_guide}")

                        
                    else:
                        dl_log.debug(f"skipping this item : {item}")
            else:
                converted_guide = {}
                
        else:
            converted_guide = {}
        
        return converted_guide

        

if __name__ == "__main__":
    job = LoadConvertAllDocsJob()
    job.load_convert_all_docs('TEST_SB',collection_name='subcommodity_migration')