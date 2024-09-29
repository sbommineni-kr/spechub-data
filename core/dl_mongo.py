# Audit Log
# Author: Sudheer Bommineni
# Email: sudheer.bommineni@kroger.com
# ID: KON8383
# Date: 2024-09-29
# Description: This is the mongo class for the data lake


import pymongo

class DLMongo:
    def __init__(self, connection_string, database, collection=None):
        self.connection_string = connection_string
        self.database = database
        self.collection = collection
        self.mongo_client = pymongo.MongoClient(self.connection_string)
        self.mongodb = self.mongo_client[self.database]
    
    def get_collection(self, collection=None):
        if collection is None:
            collection = self.collection
        return self.mongodb[collection]

    def get_database(self):
        return self.mongo_client[self.database]
    
    def insert_one(self, data):
        self.mongo_collection.insert_one(data)
    
    def insert_many(self, data):
        self.mongo_collection.insert_many(data)
    
    def find(self, query):
        return self.mongo_collection.find(query)
    
    def find_one(self, query):
        return self.mongo_collection.find_one(query)

    def update_one(self, query, update):
        self.mongo_collection.update_one(query, update)

    def update_many(self, query, update):
        self.mongo_collection.update_many(query, update)

    def delete_one(self, query):
        self.mongo_collection.delete_one(query)

    def delete_many(self, query):
        self.mongo_collection.delete_many(query)

    def drop(self):
        self.mongo_collection.drop()

    def count(self, query):
        return self.mongo_collection.count(query)

    def distinct(self, key):
        return self.mongo_collection.distinct(key)

    def aggregate(self, pipeline):
        return self.mongo_collection.aggregate(pipeline)

    def create_index(self, keys, unique=False):
        self.mongo_collection.create_index(keys, unique=unique)