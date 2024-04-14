import mysql.connector
from mysql.connector import Error
import random
import string

# Function to generate random string
def generate_random_string(length):
    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for _ in range(length))

# MySQL connection details
try:
    connection = mysql.connector.connect(
        host='localhost',   
        database='examenBDA',  # Replace 'mydatabase' with your database name
        user='root',    # Replace 'your_username' with your MySQL username
        password='bdaPass' # Replace 'your_password' with your MySQL password
    )

    if connection.is_connected():
        db_Info = connection.get_server_info()
        print("Connected to MySQL Server version ", db_Info)
        cursor = connection.cursor()
        
        cursor.execute("CREATE DATABASE IF NOT EXISTS examenBDA")  # Replace 'mydatabase' with your database name
        cursor.execute("USE examenBDA")

        # Create customers table if not exists
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS customers (
                customer_id INT PRIMARY KEY,
                name VARCHAR(255),
                email VARCHAR(255),
                address VARCHAR(255),
                phone VARCHAR(20)
            )
        """)

        # Create orders table if not exists
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS orders (
                order_id INT PRIMARY KEY,
                customer_id INT,
                order_date DATE,
                total_amount DECIMAL(10, 2),
                FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
            )
        """)

        # Create order_items table if not exists
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS order_items (
                order_item_id INT PRIMARY KEY,
                order_id INT,
                product_id INT,
                quantity INT,
                unit_price DECIMAL(10, 2),
                FOREIGN KEY (order_id) REFERENCES orders(order_id)
            )
        """)

        connection.commit()
        print("Tables created successfully in MySQL.")

        # Generate sample data for customers table
        customers_data = []
        for i in range(1, 1001):
            customer = (i, f'Customer{i}', f'customer{i}@example.com', f'{i} Main St', f'{random.randint(100, 999)}-{random.randint(100, 999)}-{random.randint(1000, 9999)}')
            customers_data.append(customer)

        # Generate sample data for orders table
        orders_data = []
        for i in range(1, 1001):
            order = (i, random.randint(1, 1000), '2024-02-19', round(random.uniform(50.0, 500.0), 2))
            orders_data.append(order)

        # Generate sample data for order_items table
        order_items_data = []
        for i in range(1, 1001):
            order_item = (i, i, random.randint(1, 1000), random.randint(1, 10), round(random.uniform(10.0, 100.0), 2))
            order_items_data.append(order_item)

        # Insert data into MySQL tables
        customers_query = "INSERT INTO customers (customer_id, name, email, address, phone) VALUES (%s, %s, %s, %s, %s)"
        orders_query = "INSERT INTO orders (order_id, customer_id, order_date, total_amount) VALUES (%s, %s, %s, %s)"
        order_items_query = "INSERT INTO order_items (order_item_id, order_id, product_id, quantity, unit_price) VALUES (%s, %s, %s, %s, %s)"

        cursor.executemany(customers_query, customers_data)
        cursor.executemany(orders_query, orders_data)
        cursor.executemany(order_items_query, order_items_data)

        connection.commit()
        print("Data inserted successfully into MySQL.")

except Error as e:
    print("Error while connecting to MySQL", e)

finally:
    if connection.is_connected():
        cursor.close()
        connection.close()
        print("MySQL connection is closed.")