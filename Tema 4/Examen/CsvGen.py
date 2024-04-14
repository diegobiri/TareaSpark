import pymongo
import mysql.connector
from neo4j import GraphDatabase
import csv

# MongoDB connection details
mongo_client = pymongo.MongoClient("mongodb://localhost:27017/")
mongo_db = mongo_client["ExamenBDA"]  # Replace 'mydatabase' with your MongoDB database name
mongo_users_collection = mongo_db["users"]

# MySQL connection details
mysql_connection = mysql.connector.connect(
    host='localhost',   
    database='examenBDA',  # Replace 'mydatabase' with your database name
    user='root',    # Replace 'your_username' with your MySQL username
    password='bdaPass' # Replace 'your_password' with your MySQL password
)
mysql_cursor = mysql_connection.cursor()

# Neo4j connection details
neo4j_driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "gong-ski-source-prince-mango-2725"))  # Replace 'your_password' with your Neo4j password

# Function to fetch data from MongoDB
def fetch_mongo_users():
    return list(mongo_users_collection.find({}, {'_id': 1, 'username': 1, 'email': 1, 'age': 1, 'country': 1}))

# Function to fetch data from MySQL
def fetch_mysql_orders():
    mysql_cursor.execute("SELECT * FROM orders")
    return mysql_cursor.fetchall()

# Function to fetch data from Neo4j
def fetch_neo4j_order_items():
 with neo4j_driver.session() as session:
        result = session.run("MATCH (u:User)-[:PLACED]->(o:Order)-[c:CONTAINS]->(p:Product) RETURN o.id AS order_id, p.id AS order_item_id, o.total_amount AS quantity, p.price AS unit_price")
        return list(result)

# Generate combined data
combined_data = []
mongo_users = fetch_mongo_users()
mysql_orders = fetch_mysql_orders()
neo4j_order_items = fetch_neo4j_order_items()

# Merge data from different databases
for order in mysql_orders:
    order_id, customer_id, order_date, total_amount = order
    for item in neo4j_order_items:
        if item['order_id'] == order_id:
            order_item_id = item['order_item_id']
            quantity = item['quantity']
            unit_price = item['unit_price']
            for user in mongo_users:
                if user['_id'] == customer_id:
                    user_id = user['_id']
                    username = user['username']
                    email = user['email']
                    age = user['age']
                    country = user['country']
                    combined_data.append((order_id, order_date, total_amount, order_item_id, quantity, unit_price, user_id, username, email, age, country))
                    break

# Write data to CSV file
with open('combined_data.csv', 'w', newline='') as csvfile:
    csv_writer = csv.writer(csvfile)
    csv_writer.writerow(['order_id', 'order_date', 'total_amount', 'order_item_id', 'quantity', 'unit_price', 'user_id', 'username', 'email', 'age', 'country'])
    csv_writer.writerows(combined_data)

print("CSV file generated successfully.")