from neo4j import GraphDatabase
import random
import string

# Function to generate random string
def generate_random_string(length):
    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for _ in range(length))

# Neo4j connection details
uri = "bolt://localhost:7687"
username = "neo4j"
password = "gong-ski-source-prince-mango-2725"  # Replace 'your_password' with your Neo4j password

# Function to create nodes and relationships in Neo4j
def create_data(tx):
    # Create Users
    for i in range(1, 1001):
        tx.run("CREATE (:User {id: $id, name: $name, email: $email})", id=i, name=f"User{i}", email=f"user{i}@example.com")

    # Create Products
    for i in range(1, 1001):
        tx.run("CREATE (:Product {id: $id, name: $name, description: $description, price: $price, category: $category})", id=i, name=f"Product{i}", description=generate_random_string(20), price=random.uniform(10.0, 100.0), category=random.choice(["Electronics", "Clothing", "Books", "Home Decor"]))

    # Create Orders and relationships
    for i in range(1, 1001):
        tx.run("MATCH (u:User {id: $user_id}), (p:Product {id: $product_id}) "
               "CREATE (u)-[:PLACED]->(o:Order {id: $order_id, order_date: $order_date, total_amount: $total_amount})-[:CONTAINS]->(p)",
               user_id=random.randint(1, 1000),
               product_id=random.randint(1, 1000),
               order_id=i,
               order_date="2024-02-19",
               total_amount=random.uniform(50.0, 500.0))

# Connect to Neo4j and insert data
driver = GraphDatabase.driver(uri, auth=(username, password))
with driver.session() as session:
    session.write_transaction(create_data)

print("Data inserted successfully into Neo4j.")