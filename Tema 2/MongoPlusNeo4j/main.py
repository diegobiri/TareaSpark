from data.mongo_operations import MongoDBOperations
from data.neo4j_operations import Neo4jCRUD
from model.book import Book
from model.newBook import NewBook
import random
import string

def get_random_string(length):
    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for i in range(length))

#Nos conectamos a las BBDD
mongo_operations = MongoDBOperations('Library', 'book','32768')
neo4j_crud = Neo4jCRUD("bolt://localhost:7687", "neo4j", "password")

#Añadimos datos a los libros, no ejecutar en caso de haberse realizado antes
'''
for i in range(0, 30):
    book = NewBook(name=get_random_string(8),
                         ISBN=get_random_string(6),
                         author=get_random_string(4),
                         idProducto=i)
    mongo_operations.create_book(book)
'''

#obtenemos data libros y productos
all_books = mongo_operations.read_books({})
all_products=neo4j_crud.read_nodes("Product")

for libro in all_books:
    for producto in all_products:
        if libro.idProducto == int(producto._properties["productID"]):
            print (f'El libro {libro.name} tiene en inventario {producto._properties["unitsInStock"]} libro.idProducto')



