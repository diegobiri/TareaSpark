class Book:
    title: str
    author: str
    isbn: str
    
    def __init__(self, title, author, isbn):
        self.title = title
        self.author = author
        self.isbn = isbn
        
    def to_dict(self):
        return {"title": self.title, "author": self.author, "isbn": self.isbn}
    
    @classmethod
    def from_dict(cls, person_dict):
        return cls(person_dict["title"], person_dict["author"], person_dict["isbn"] )
    
