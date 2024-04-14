from pymongo import MongoClient
import json

def read_json_file(filename):
    try:
        with open(filename, 'r') as file:
            data = json.load(file)
            return data
    except FileNotFoundError:
        return None
    
class MongoDBOperations:
    def __init__(self, database_name, port,username=None, password=None):
        if username and password:
            self.client = MongoClient(f'mongodb://{username}:{password}@localhost:{{port}}/')
        else:
            self.client = MongoClient(f'mongodb://localhost:{port}/')
        self.db = self.client[database_name]
        
    def create_person(self, collection_name,data):
        self.collection = self.db[collection_name]
        result = self.collection.insert_one(data)
        return result
    
destinations=read_json_file("Mongo/destinations_mongo.json")
bookings=read_json_file("Mongo/bookings_mongo.json")
accommodations=read_json_file("Mongo/accommodations_mongo.json")


mongo_operations = MongoDBOperations('Acomodations','32769')

for data in destinations:
    mongo_operations.create_person("destinations",data)

for data in bookings:
    mongo_operations.create_person("bookings",data)

for data in accommodations:
    mongo_operations.create_person("accommodations",data)


