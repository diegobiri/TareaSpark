from model.newBook import NewBook

class Book(NewBook):
    def __init__(self, name, ISBN, author,idProducto,_id=None):
        super().__init__(name, ISBN, author,idProducto)
        self._id=_id
    def __str__(self) -> str:
        return f"id: {self._id}, {super().__str__()}"
    def __repr__(self):
        return self.__str__()