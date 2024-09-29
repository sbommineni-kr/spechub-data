# Audit Log
# Author: Sudheer Bommineni
# Email: sudheer.bommineni@kroger.com
# ID: KON8383
# Date: 2024-09-29
# Description: This is a python class that reads the config file and returns the config object

import json 
import os
import yaml

class DLConfig:
    def __init__(self, config_file):
        self.config_file = config_file
        self.config = self.read_config()
    
    def __is_valid_config(self, config_file_path):

        is_valid_config = None
        
        #check if the file exists 
        if not os.path.exists(config_file_path):
            is_valid_config = False
            raise FileNotFoundError(f"Config file {config_file_path} not found.")
        else:
            is_valid_config = True
        
        #check if the file is a valid yaml file
        if not config_file_path.endswith('.yaml'):
            is_valid_config = False
            raise ValueError(f"Config file {config_file_path} is not a valid YAML file.")
        else:
            is_valid_config = True
        
        return is_valid_config
        
        

    
    def read_config(self):
        #check if the file is valid
        if self.__is_valid_config(self.config_file):
            
            with open(self.config_file, 'r') as file:
                config = yaml.safe_load(file)
            
            return config