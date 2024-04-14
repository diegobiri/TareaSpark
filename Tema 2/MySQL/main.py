#pip install mysql-connector-python

from data.db_operations import Database
from model.employee import Employee

DB_HOST = ""
DB_USER = ""
DB_PASSWORD = ""
DB_DATABASE = ""
DB_PORT= ""


db = Database(DB_HOST, DB_USER, DB_PASSWORD, DB_DATABASE,DB_PORT)


db.create_table()

employeeJohn = Employee("John Doe", 30, "IT")
employeeSmith = Employee("Jane Smith", 25, "HR")


employeeJohn=db.insert_data(employeeJohn)
employeeSmith=db.insert_data(employeeSmith)


all_data = db.get_all_data()
print("All Data:")
for row in all_data:
    print(row)


employeeJohn.name = "Updated John Doe"
db.update_data(employeeJohn)


updated_data = db.get_all_data()
print("\nUpdated Data:")
for row in updated_data:
    print(row)


db.delete_data(employeeSmith.id)


after_deletion_data = db.get_all_data()
print("\nData After Deletion:")
for row in after_deletion_data:
    print(row)

db.close_connection()

