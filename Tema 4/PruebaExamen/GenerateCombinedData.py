from pymongo import MongoClient
import csv
import mysql.connector
from neo4j import GraphDatabase


def create_csv_file(filename,data):
    with open(filename, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerows(data)
        
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
    
    def get_all_data(self,tabla):
        select_all_query = f"SELECT * FROM {tabla}"
        self.cursor.execute(select_all_query)
        result = self.cursor.fetchall()
        return list(result)
        
class MongoDBOperations:
    def __init__(self, database_name, port,username=None, password=None):
        if username and password:
            self.client = MongoClient(f'mongodb://{username}:{password}@localhost:{{port}}/')
        else:
            self.client = MongoClient(f'mongodb://localhost:{port}/')
        self.db = self.client[database_name]
        self.collection = self.db["empleado"]
    
    def read_Collection(self, filter_criteria):
        result = self.collection.find(filter_criteria)
        return list(result)
    
    def change_Collection(self, collection_name):
        result = self.collection=self.db[collection_name]
        return result
        
class Neo4jCRUD:
    def __init__(self, uri, user, password):
        self._uri = uri
        self._user = user
        self._password = password
        self._driver = None
        self._connect()

    def _connect(self):
        self._driver = GraphDatabase.driver(self._uri, auth=(self._user, self._password))
    
    def read_nodes(self, label):
        with self._driver.session() as session:
            result = session.read_transaction(self._read_nodes, label)
            return result

    @staticmethod
    def _read_nodes(tx, label):
        query = (
            f"MATCH (n:{label}) " 
            "RETURN n"
        )
        result = tx.run(query)
        return [record["n"] for record in result]
    
    def close(self):
        if self._driver is not None:
            self._driver.close()
            

DB_HOST = "localhost"
DB_USER = "root"
DB_PASSWORD = "my-secret-pw"
DB_DATABASE = "Empresa"
DB_PORT= "8888"

db_SQL = Database(DB_HOST, DB_USER, DB_PASSWORD, DB_DATABASE,DB_PORT)
 
mongo_operations = MongoDBOperations('BDAExamen','27017')

uri = "bolt://localhost:7687"  
user = "neo4j"
password = "palace-smoke-wedding-pardon-nothing-5303"

neo4j_crud = Neo4jCRUD(uri, user, password)

#Consultas Neo4j
proyectos=neo4j_crud.read_nodes("Proyectos")
departamentos=neo4j_crud.read_nodes("Departamento")

#Consultas MongoDB
empleados= mongo_operations.read_Collection({})

#Consultas SQL
facturas=db_SQL.get_all_data("Facturas")

data = [["Departamento", "NumEmpleados","NumProyectos", "NumClientes","Importe"]]

for departamento in departamentos:
    nombreDepartmento=departamento._properties["nombre"]
    numProyectos=0
    clientesDistintos= []
    for proyecto in proyectos:
        if (departamento._properties["id"]==proyecto._properties["departamento"]):
            numProyectos+=1
            if (proyecto._properties["cliente"] not in clientesDistintos):
                clientesDistintos.append(int(proyecto._properties["cliente"]))
    
    importe=0
    for factura in facturas:
        if (factura[2] in clientesDistintos):
            importe+=factura[3]
            
    numClientes=len(clientesDistintos)
    numEmpleados=0     
    for empleado in empleados:
        if (int(departamento._properties["id"])==empleado["idDepartamento"]):
            numEmpleados+=1
            
    data.append([nombreDepartmento,numEmpleados,numProyectos,numClientes,importe])
    
create_csv_file("ExtractTransformedData.csv",data)
    
    
    
    
    
            