import pymongo
import random
import string

# MongoDB connection details
mongo_client = pymongo.MongoClient("mongodb://localhost:27017/")
db = mongo_client["ExamenBDA"]  # Replace 'mydatabase' with your database name

# Function to generate random string
def generate_random_string(length):
    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for _ in range(length))

# Generate sample data for users collection
users_data = []
for i in range(1, 1001):
    user = {
        "_id": i,
        "username": f"user{i}",
        "email": f"user{i}@example.com",
        "age": random.randint(18, 60),
        "country": random.choice(["USA", "UK", "Canada", "Australia"])
    }
    users_data.append(user)

# Generate sample data for products collection
products_data = []
for i in range(1, 1001):
    product = {
        "_id": i,
        "name": f"Product{i}",
        "description": generate_random_string(20),
        "price": round(random.uniform(10.0, 100.0), 2),
        "category": random.choice(["Electronics", "Clothing", "Books", "Home Decor"])
    }
    products_data.append(product)

# Insert data into MongoDB collections
users_collection = db["users"]
products_collection = db["products"]

users_collection.insert_many(users_data)
products_collection.insert_many(products_data)

print("Data inserted successfully into MongoDB.")
