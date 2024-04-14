import csv
import random
from datetime import datetime, timedelta
from decimal import Decimal

def generate_random_date(start_date, end_date):
    delta = end_date - start_date
    random_days = random.randint(0, delta.days)
    return start_date + timedelta(days=random_days)

def generate_random_string(length=8, alphanumeric=True):
    if alphanumeric:
        letters = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789'
    else:
        letters = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ'
    return ''.join(random.choice(letters) for _ in range(length))

def generate_random_decimal(max_value=1000, with_error=False):
    if with_error:
        return Decimal(random.uniform(-1000, max_value))  # Intentional potential negative number for errors
    return Decimal(random.uniform(1, max_value))

def introduce_errors(row):
    error_types = [None, "", "NaN", "null"]
    # Introduce an error or null in a random column (except date)
    if random.choice([True, False]):  # 50% chance to introduce an error or null
        row[random.randint(1, len(row)-1)] = random.choice(error_types)
    return row

# Define column names
columns = ["Date", "Store ID", "Product ID", "Quantity Sold", "Revenue"]

# Generate random data for each row
data = []
start_date = datetime(2020, 1, 1)
end_date = datetime(2025, 12, 31)
num_rows = 1000

for _ in range(num_rows):
    row = [
        generate_random_date(start_date, end_date).strftime('%Y-%m-%d'),
        random.randint(1, 100),
        generate_random_string(10, alphanumeric=True),
        random.randint(1, 100),
        float(generate_random_decimal(with_error=random.choice([True, False]))),
    ]
    row = introduce_errors(row)  # Introduce errors with a 50% chance
    data.append(row)

# Write data to CSV file
with open('sales_data.csv', 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(columns)
    writer.writerows(data)

print("CSV file generated successfully with random errors and nulls.")

