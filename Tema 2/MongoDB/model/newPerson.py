class NewPerson:
    def __init__(self, name, age, email):
        self.name = name
        self.age = age
        self.email = email
    def __str__(self) -> str:
        return f"name: {self.name}, age: {self.age} email: {self.email}"
    def __repr__(self):
        return self.__str__()