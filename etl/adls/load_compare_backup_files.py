# Audit Log
# Author: Sudheer Bommineni
# Email: sudheer.bommineni@kroger.com
# ID: KON8383
# Date: 2024-12-02
# Description: This script is used to compare backup files of two dates in ADLS.
# Execution : python /Users/sudheer.bomminenikroger.com/Desktop/work/git/spechub-data/etl/adls/load_compare_backup_files.py -c config/datalake.yaml -s 20241202 -e 20241203 -a '{"storage_connection":"","subCommodityCode":"98046","send_email":"False","email_password":""}' -n load_compare_backup_files -v prod

from core.dl_etlbase import DLETLBase
import os 
import json
import yaml

from bson.json_util import dumps
from pymongo.errors import OperationFailure
from core.dl_adls import DLAzureDataLake
from core.dl_logger import DLLogger
from core.dl_email  import DLEmail


class CompareBackupFiles(DLETLBase):
    def __init__(self):
        super().__init__()
    
    def get_yaml_args(self):

        # Get job yaml args
        job_yaml_args = self.get_job_yaml_args()
        
        # Read the arguments from yaml config
        storage_connection = job_yaml_args['arguments']['storage_connection']
        container_name = job_yaml_args['arguments']['container_name'] 
        file_names = job_yaml_args['arguments']['file_names']
        email = job_yaml_args['arguments']['email']
        
        # Allow override from command line args if provided
        if 'storage_connection' in self.job_additional_args:
            storage_connection = self.job_additional_args['storage_connection']
        if 'container_name' in self.job_additional_args:
            container_name = self.job_additional_args['container_name']
        if 'file_names' in self.job_additional_args:
            file_names = self.job_additional_args['file_names']
        if 'subCommodityCode' in self.job_additional_args:
            subCommodityCode = self.job_additional_args['subCommodityCode']
        else:
            subCommodityCode = None
        if 'send_email' in self.job_additional_args:
            send_email = self.job_additional_args['send_email']
        else:
            send_email = "False"
        if 'email_password' in self.job_additional_args:
            email_password = self.job_additional_args['email_password']
        elif 'EMAIL_PASSWORD' in os.environ:
            email_password = os.environ['EMAIL_PASSWORD']
        else:
            email_password = None
        
        return storage_connection, container_name, file_names, email, subCommodityCode, send_email, email_password

    def send_email(self, email_to, subject, body):
        pass
    
    def __compare_files(self, start_date_local_path, end_date_local_path, subCommodityCode):
        # Load both JSON files
        dl_log.info(f"Starting comparison between {start_date_local_path} and {end_date_local_path}")

        #just get the filenames from the paths 
        start_date_filename = os.path.basename(start_date_local_path)
        end_date_filename = os.path.basename(end_date_local_path)
        
        # Create CSV file for logging differences
        if subCommodityCode:
            csv_log_file = f"data/comparison_log_{subCommodityCode}_{start_date_filename}_{end_date_filename}.csv"
        else:
            csv_log_file = f"data/comparison_log_{start_date_filename}_{end_date_filename}.csv"
        
        with open(csv_log_file, 'w') as csvfile:
            csvfile.write("subCommodityCode,path,start_date_value,end_date_value\n")
    
        with open(start_date_local_path, 'r') as start_file:
            start_data = json.load(start_file)
            
            if subCommodityCode:
                start_data = [item for item in start_data if item.get('subCommodityCode') == subCommodityCode]

                if start_data == []:
                    with open(start_date_local_path, 'r') as start_file:
                        start_data = json.load(start_file)
            else:
                start_data = start_data
            
            dl_log.info(f"Loaded {len(start_data) if isinstance(start_data, list) else 1} documents from {start_date_local_path}")
    
        with open(end_date_local_path, 'r') as end_file:
            end_data = json.load(end_file)
            
            if subCommodityCode:
                end_data = [item for item in end_data if item.get('subCommodityCode') == subCommodityCode]
                
                if end_data == []:
                    with open(end_date_local_path, 'r') as end_file:
                        end_data = json.load(end_file)
            else:
                end_data = end_data

            dl_log.info(f"Loaded {len(end_data) if isinstance(end_data, list) else 1} documents from {end_date_local_path}")

        def get_subcommodity_code(data):
            if isinstance(data, dict):
                return data.get('subCommodityCode', 'N/A')
            return 'N/A'

        def compare_values(path, value1, value2, subcommodity_code):
            
            dl_log.debug(f"Comparing values at path: {path}")
            
            if isinstance(value1, dict) and isinstance(value2, dict):
                dl_log.debug(f"Found dictionary at {path}, comparing nested structure")
                compare_dict(f"{path}", value1, value2, subcommodity_code)
            
            if isinstance(value1, list) and isinstance(value2, list):
                dl_log.debug(f"Found list at {path} with lengths {len(value1)} and {len(value2)}")
                if len(value1) != len(value2):
                    dl_log.warning(f"Different list lengths at {path}: {len(value1)} vs {len(value2)}")
                    with open(csv_log_file, 'a') as csvfile:
                        csvfile.write(f"{subcommodity_code},{path},length:{len(value1)},length:{len(value2)}\n")
                for i in range(min(len(value1), len(value2))):
                    dl_log.debug(f"Comparing list item {i} at {path}")
                    compare_values(f"{path}[{i}]", value1[i], value2[i], subcommodity_code)
            
            elif value1 != value2:
                dl_log.warning(f"Value mismatch at {path}:")
                dl_log.warning(f"Start date value: {value1}")
                dl_log.warning(f"End date value: {value2}")
                with open(csv_log_file, 'a') as csvfile:
                    csvfile.write(f"{subcommodity_code},{path},{value1},{value2}\n")

        def compare_dict(path, dict1, dict2, subcommodity_code):
            dl_log.debug(f"Comparing dictionary at path: {path}")
            for key in dict1:
                if key not in dict2:
                    dl_log.warning(f"Key '{key}' at {path} exists in start date file but not in end date file")
                    with open(csv_log_file, 'a') as csvfile:
                        csvfile.write(f"{subcommodity_code},{path}.{key},exists,missing\n")
                else:
                    compare_values(f"{path}.{key}", dict1[key], dict2[key], subcommodity_code)
        
            for key in dict2:
                if key not in dict1:
                    dl_log.warning(f"Key '{key}' at {path} exists in end date file but not in start date file")
                    with open(csv_log_file, 'a') as csvfile:
                        csvfile.write(f"{subcommodity_code},{path}.{key},missing,exists\n")

        # Start the comparison
        dl_log.info("Starting document comparison")
        if isinstance(start_data, list) and isinstance(end_data, list):
            dl_log.info(f"Comparing {len(start_data)} documents against {len(end_data)} documents")
            for i in range(min(len(start_data), len(end_data))):
                subcommodity_code = get_subcommodity_code(start_data[i])
                dl_log.info(f"Comparing document {i+1} of {min(len(start_data), len(end_data))} with subCommodityCode: {subcommodity_code}")
                compare_values(f"root[{i}]", start_data[i], end_data[i], subcommodity_code)
        else:
            subcommodity_code = get_subcommodity_code(start_data)
            dl_log.info(f"Comparing single document with subCommodityCode: {subcommodity_code}")
            compare_dict("root", start_data, end_data, subcommodity_code)

        dl_log.info("Comparison completed")
        dl_log.info("Cleaning up temporary files")
    
        # Clean up the local files
        os.remove(start_date_local_path)
        os.remove(end_date_local_path)
        dl_log.info(f"Temporary files removed. Comparison log saved to {csv_log_file}")

        #return the log file path
        return csv_log_file
    
    def compare_backup_files(self, container_name, file_names, connection_string, subCommodityCode, email, send_email, email_password):

        # Create an instance of DLAzureDataLake
        adls = DLAzureDataLake(connection_string,container_name)

        #build the path to get the files from ADLS
        file_paths = []
        for file_name in file_names:
            start_date_blob_name = f"{self.env}/{self.start_date}/{file_name}"
            end_date_blob_name = f"{self.env}/{self.end_date}/{file_name}"

            #list the files from ADLS and throw an error if the file is not found
            start_date_files = adls.list_files(start_date_blob_name)
            
            if not start_date_files:
                raise ValueError(f"File {start_date_blob_name} not found in ADLS")
            
            end_date_files = adls.list_files(end_date_blob_name)
            if not end_date_files:
                raise ValueError(f"File {end_date_blob_name} not found in ADLS")
            
            #download the files from ADLS
            start_date_local_path = f"data/{self.start_date}_{file_name}"
            end_date_local_path = f"data/{self.end_date}_{file_name}"
            
            adls.download_file(start_date_blob_name, start_date_local_path)
            adls.download_file(end_date_blob_name, end_date_local_path)
                                                  
            
            csv_log_file = self.__compare_files(start_date_local_path, end_date_local_path, subCommodityCode)

            if send_email.lower() == "true":
                subject = email["subject"].format(previous_day=self.start_date,current_day=self.end_date)
                body = email["body"]
                to = email["to"]
                from_address = email["from"]
                if not email_password:
                    raise ValueError("Email password is required")
                email = DLEmail(user=from_address, password=email_password)
                email.send_email(from_address, to, subject, body, csv_log_file)

            


if __name__ == "__main__":

    job = CompareBackupFiles()
    dl_log = DLLogger(job.job_name)

    storage_connection, container_name, file_names, email, subCommodityCode, send_email, email_password = job.get_yaml_args()
    job.compare_backup_files(container_name, file_names, storage_connection, subCommodityCode, email, send_email, email_password)