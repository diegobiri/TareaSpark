import psycopg2
import psycopg2.extras  
import random
from datetime import datetime, timedelta

def generate_random_date(start_date, end_date):
    delta = end_date - start_date
    random_days = random.randint(0, delta.days)
    return start_date + timedelta(days=random_days)

def generate_random_string(length=10):
    return ''.join(random.choice('abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ') for _ in range(length))

def connect_db():
    return psycopg2.connect(
        host="localhost",
        database="retail_db",
        user="postgres",
        password="casa1234",
        port=5432
    )

def create_table(conn):
    with conn.cursor() as cursor:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS stores (
                store_id SERIAL PRIMARY KEY,
                store_name VARCHAR(255),
                location VARCHAR(255),
                demographics JSON
            );
        """)
        conn.commit()

def generate_data(num_rows):
    data = []
    for _ in range(num_rows):
        store_name = generate_random_string()
        location = generate_random_string()
        demographics = {"age_range": f"{random.randint(20, 40)}-{random.randint(41, 60)}", "income_bracket": random.choice(["low", "medium", "high"])}
        data.append((store_name, location, demographics))
    return data

def insert_data(conn, data):
    with conn.cursor() as cursor:
        psycopg2.extras.execute_batch(cursor, """
            INSERT INTO stores (store_name, location, demographics) VALUES (%s, %s, %s);
        """, data)
        conn.commit()

def main():
    conn = connect_db()
    create_table(conn)
    data = generate_data(1000)  # Generate 1000 rows of data
    insert_data(conn, data)
    print("Data inserted into PostgreSQL database successfully.")
    conn.close()

if __name__ == '__main__':
    main()
