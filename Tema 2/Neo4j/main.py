#pip install neo4j
from data.neo4j_operations import Neo4jCRUD

def main():
    uri = "bolt://localhost:7687"  
    user = "neo4j"
    password = "password"

    neo4j_crud = Neo4jCRUD(uri, user, password)

    # Example: Create a node
    node_properties = {"name": "Node 1", "type": "Example", "cantidad":12}
    created_node = neo4j_crud.create_node("ExampleNode", node_properties)
    print(f"Created Node: {created_node}")

    # Example: Read nodes
    nodes = neo4j_crud.read_nodes("ExampleNode")
    print("Nodes:")
    for node in nodes:
        print(node)

    # Example: Update node
    update_properties = {"name": "Updated Node 1"}
    updated_node = neo4j_crud.update_node(created_node.id, update_properties)
    print(f"Updated Node: {updated_node}")

    # Example: Remove node
    neo4j_crud.remove_node(created_node.id)
    print("Node removed.")

    # Close the connection when done
    neo4j_crud.close()
    
    
if __name__ == "__main__":
    main()
