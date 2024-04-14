from neo4j import GraphDatabase
import csv
def read_csv_file(filename):
    data =[]
    with open(filename, 'r',encoding="utf-8") as file:
        reader= csv.reader(file)
        for element in reader:
            data.append(element)        
    return data

class Neo4jCRUD:
    def __init__(self, uri, user, password):
        self._uri = uri
        self._user = user
        self._password = password
        self._driver = None
        self._connect()

    def _connect(self):
        self._driver = GraphDatabase.driver(self._uri, auth=(self._user, self._password))

    def close(self):
        if self._driver is not None:
            self._driver.close()

    def create_node(self, label, properties):
        with self._driver.session() as session:
            result = session.write_transaction(self._create_node, label, properties)
            return result

    @staticmethod
    def _create_node(tx, label, properties):
        query = (
            f"CREATE (n:{label} $props) "
            "RETURN n"
        )
        result = tx.run(query, props=properties)
        
    
    def create_relationship(self,labelOrigin,propertyOrigin,labelEnd,relationshipName):
         with self._driver.session() as session:
            result = session.write_transaction(self._create_relationship, labelOrigin,propertyOrigin,labelEnd,relationshipName)
            return result
        
    @staticmethod
    def _create_relationship(tx, labelOrigin,propertyOrigin,labelEnd,relationshipName):
        query = (
            f"MATCH (n:{labelOrigin}),(c:{labelEnd}) "
            f"WHERE n.id='{propertyOrigin}' and c.departamento='{propertyOrigin}' " 
            f"CREATE (n)-[:{relationshipName}]->(c)"
        )
        result = tx.run(query)
        return result
    
uri = "bolt://localhost:7687"  
user = "neo4j"
password = "palace-smoke-wedding-pardon-nothing-5303"

neo4j_crud = Neo4jCRUD(uri, user, password)

departamentos= read_csv_file("Tema 4/PruebaExamen/data/Neo4j/departamento.csv")
proyectos=read_csv_file("Tema 4/PruebaExamen/data/Neo4j/proyecto.csv")

for element in departamentos[1:]:
    node_properties = {
        "id": element[0], 
        "nombre": element[1],
        "area":element[2]
        }
    neo4j_crud.create_node("Departamento", node_properties)

for element in proyectos[1:]:
    node_properties = {
        "id": element[0], 
        "Nombre": element[1],
        "cliente":element[2],
        "empleadoResponsable":element[3],
        "departamento":element[4]
        }
    neo4j_crud.create_node("Proyectos", node_properties)

for element in proyectos[1:]:
    neo4j_crud.create_relationship("Departamento",element[4], "Proyectos","TIENE_UN")
    