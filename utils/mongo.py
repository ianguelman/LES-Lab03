from ast import List
import os
from pydoc import doc
from typing import Collection
import pymongo

class Mongo:
    
    __client: any
    __database: any
    __collection: any
    
    def __init__(self):
        self.__client = pymongo.MongoClient(
            host=os.environ['DATABASE_HOST'],
            port=int(os.environ['DATABASE_PORT']),
            username=os.environ['DATABASE_USERNAME'],
            password=os.environ['DATABASE_PASSWORD'],
        )
        self.__database = self.__client[os.environ['PRIMARY_DATABASE']]


    def set_collection(self, collection):
        if collection == 'repo':
           self.__collection = self.__database[os.environ['REPO_COLLECTION']]
        elif collection == 'pr':
           self.__collection = self.__database[os.environ['PR_COLLECTION']]
        elif collection == 'config':
           self.__collection = self.__database[os.environ['CONFIG_COLLECTION']] 
        
    def insert_one(self, value, collection):
        self.set_collection(collection)
        self.__collection.insert_one(value)
        
    def insert_many(self, value, collection):
        self.set_collection(collection)
        self.__collection.insert_many(value)

    def update_one(self, query, value, collection):
        self.set_collection(collection)
        self.__collection.update_one(query, value, upsert=True)

    def get_documents_count(self, collection):
        self.set_collection(collection)
        return self.__collection.count_documents({})

    def get_processed_documents_count(self, collection):
        self.set_collection(collection)
        return self.__collection.count_documents({"processed":True})
    
    def get_document(self, query, collection):
        self.set_collection(collection)
        return self.__collection.find(query)[0]

    def get_all_documents(self, collection):
        self.set_collection(collection)
        documents = []
        for document in self.__collection.find({}):
          documents.append(document)
        return documents