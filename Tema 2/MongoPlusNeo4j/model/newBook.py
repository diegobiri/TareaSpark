class NewBook:
    def __init__(self, name, ISBN, author,idProducto):
        self.name = name
        self.ISBN = ISBN
        self.author = author
        self.idProducto=idProducto
    def __str__(self) -> str:
        return f"name: {self.name}, ISBN: {self.ISBN} author: {self.author} idProducto: {self.idProducto}"
    def __repr__(self):
        return self.__str__()