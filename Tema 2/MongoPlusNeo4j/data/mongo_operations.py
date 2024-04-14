from pymongo import MongoClient
from model.book import Book
from model.newBook import NewBook

class MongoDBOperations:
    def __init__(self, database_name, collection_name, port,username=None, password=None):
        if username and password:
            self.client = MongoClient(f'mongodb://{username}:{password}@localhost:{{port}}/')
        else:
            self.client = MongoClient(f'mongodb://localhost:{port}/')
            
        self.db = self.client[database_name]
        self.collection = self.db[collection_name]

    def create_book(self, book: NewBook):
        result = self.collection.insert_one(book.__dict__)
        return result

    def read_books(self, filter_criteria):
        result = self.collection.find(filter_criteria)
        books = [Book(**book) for book in result]
        return books

    def update_book(self, filter_criteria, update_data):
        result = self.collection.update_many(filter_criteria, {'$set': update_data})
        return result.modified_count

    def delete_book(self, filter_criteria):
        result = self.collection.delete_many(filter_criteria)
        return result.deleted_count
    
    def run_aggregation(self, pipeline):
        result = self.collection.aggregate(pipeline)
        return list(result)