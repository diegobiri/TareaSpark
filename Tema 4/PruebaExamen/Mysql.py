import csv
import mysql.connector
create_table_queryFacturas = """
        CREATE TABLE IF NOT EXISTS Facturas (
            id INT PRIMARY KEY AUTO_INCREMENT,
            proyectoId INT,
            idCliente INT,
            Importe decimal(16,4)
        )
        """
        
create_table_queryClientes = """
        CREATE TABLE IF NOT EXISTS Clientes (
            id INT PRIMARY KEY AUTO_INCREMENT,
            nombre nvarchar(40),
            apellidos nvarchar(40)
        )
        """

def read_csv_file(filename):
    data =[]
    with open(filename, 'r') as file:
        reader= csv.reader(file)
        for element in reader:
            data.append(element)        
    return data

class Database:
    def __init__(self, host, user, password, database,port):
        self.connection = mysql.connector.connect(
            host=host,
            user=user,
            port=port,
            password=password,
            database=database
        )
        self.cursor = self.connection.cursor()

    def create_table(self,stringCreate):
        self.cursor.execute(stringCreate)
        self.connection.commit()

    def insert_data(self, query,params):
        self.cursor.execute(query, params)
        self.connection.commit()
        
            
            
facturas= read_csv_file("Tema 4/PruebaExamen/data/MySQL/facturas.csv")
clientes=read_csv_file("Tema 4/PruebaExamen/data/MySQL/clientes.csv")

DB_HOST = "localhost"
DB_USER = "root"
DB_PASSWORD = "my-secret-pw"
DB_DATABASE = "Empresa"
DB_PORT= "8888"

db = Database(DB_HOST, DB_USER, DB_PASSWORD, DB_DATABASE,DB_PORT)
db.create_table(create_table_queryFacturas)
db.create_table(create_table_queryClientes)
        
for element in facturas[1:]:
    insert_query = "INSERT INTO Facturas (proyectoId,idCliente,Importe) VALUES (%s, %s, %s)"
    data= (element[1],element[2],element[3])
    db.insert_data(insert_query,data)
    
for element in clientes[1:]:
    insert_query = "INSERT INTO Clientes (nombre, apellidos) VALUES (%s, %s)"
    data= (element[1],element[2])
    db.insert_data(insert_query,data)
    