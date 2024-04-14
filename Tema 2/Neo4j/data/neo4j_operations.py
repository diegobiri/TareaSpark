from neo4j import GraphDatabase

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
        return result.single()[0]

    def create_relationship(self,labelOrigin,labelEnd,relationshipName):
         with self._driver.session() as session:
            result = session.write_transaction(self._create_relationship, labelOrigin,labelEnd,relationshipName)
            return result
        
    @staticmethod
    def _create_relationship(tx, labelOrigin,propertyOrigin, labelEnd,propertyEnd,relationshipName):
        query = (
            f"MATCH (n:{labelOrigin}),(c:{labelEnd}) "
            f"WHERE n.{propertyOrigin} = c.{propertyEnd}" 
            f"CREATE (n)-[:{relationshipName}]->(c)"
        )
        result = tx.run(query)
        return result
    
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
    
    def update_node(self, node_id, properties):
        with self._driver.session() as session:
            result = session.write_transaction(self._update_node, node_id, properties)
            return result

    @staticmethod
    def _update_node(tx, node_id, properties):
        query = (
            "MATCH (n) "
            "WHERE id(n) = $node_id "
            "SET n += $props "
            "RETURN n"
        )
        result = tx.run(query, node_id=node_id, props=properties)
        return result.single()[0]

    def remove_node(self, node_id):
        with self._driver.session() as session:
            session.write_transaction(self._remove_node, node_id)

    @staticmethod
    def _remove_node(tx, node_id):
        query = (
            "MATCH (n) "
            "WHERE id(n) = $node_id "
            "DETACH DELETE n"
        )
        tx.run(query, node_id=node_id)