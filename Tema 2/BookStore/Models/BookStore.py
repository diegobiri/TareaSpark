from Models.Book import Book
from Utils.Constants import FILENAME
from Utils.JsonHelper import *
import os

class Bookstore:
    def __init__(self):
        self.books = []
        
    def dumpDataBooksToJson(self):
         booksDictionary= [person.to_dict() for person in self.books]
         create_json_file(FILENAME,booksDictionary)

         
    def loadDataBooksFromJson(self):
        if os.path.exists(FILENAME):
            data=read_json_file(FILENAME)
            self.books =[Book.from_dict(x) for x in data]


    def add_book(self, title, author, isbn):
        book = Book(title, author, isbn)
        self.books.append(book)
        self.dumpDataBooksToJson()
        
    
    def list_books(self):
        for i, book in enumerate(self.books):
            print(f"{i+1}. Title: {book.title}, Author: {book.author}, ISBN: {book.isbn}")
            
    def existBook(self, id):
        return 1 <= id <= len(self.books)
    
    def modifyBook(self, id, title):
        self.books[id-1].title=title
        self.dumpDataBooksToJson()
        
    def deleteBook(self,id):
        del self.books[id-1]
        self.dumpDataBooksToJson()
        

        