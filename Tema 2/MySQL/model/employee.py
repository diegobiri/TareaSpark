class Employee:
    def __init__(self, name, age, department, employee_id=None):
        self.id = employee_id
        self.name = name
        self.age = age
        self.department = department
    
    def __str__(self):
        return f'id: {self.id}, name: {self.name}, age: {self.age}, department: {self.department}'