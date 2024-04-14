import mysql.connector
from model.employee import Employee

class Database:
    def __init__(self, host, user, password, database,port):
        self.connection = mysql.connector.connect(
            host=host,
            user=user,
            port=port,
            password=password,
            database=database
        )
        self.cursor = self.connection.cursor()

    def create_table(self):
        create_table_query = """
        CREATE TABLE IF NOT EXISTS employees (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(255),
            age INT,
            department VARCHAR(255)
        )
        """
        self.cursor.execute(create_table_query)
        self.connection.commit()

    def insert_data(self, employee):
        insert_query = "INSERT INTO employees (name, age, department) VALUES (%s, %s, %s)"
        data = (employee.name, employee.age, employee.department)
        self.cursor.execute(insert_query, data)
        self.connection.commit()
        #Cogemos el id de la última fila insertada
        employee_id = self.cursor.lastrowid
        return Employee(employee.name, employee.age, employee.department, employee_id)

    def get_all_data(self):
        select_all_query = "SELECT * FROM employees"
        self.cursor.execute(select_all_query)
        result = self.cursor.fetchall()
        employees = []
        for row in result:
            employee = Employee(row[1], row[2], row[3], row[0])  #Cuidado con el orden
            employees.append(employee)
        return employees

    def update_data(self, employee):
        update_query = "UPDATE employees SET name=%s, age=%s, department=%s WHERE id=%s"
        data = (employee.name, employee.age, employee.department, employee.id)
        self.cursor.execute(update_query, data)
        self.connection.commit()

    def delete_data(self, employee_id):
        delete_query = "DELETE FROM employees WHERE id=%s"
        data = (employee_id,)
        self.cursor.execute(delete_query, data)
        self.connection.commit()

    def close_connection(self):
        self.cursor.close()
        self.connection.close()
