from model.newPerson import NewPerson

class Person(NewPerson):
    def __init__(self, name, age, email,_id=None):
        super().__init__(name, age, email)
        self._id=_id
    def __str__(self) -> str:
        return f"id: {self._id}, {super().__str__()}"
    def __repr__(self):
        return self.__str__()