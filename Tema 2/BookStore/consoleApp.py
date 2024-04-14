from Models.BookStore import Bookstore
def main():
    bookstore = Bookstore()
    bookstore.loadDataBooksFromJson()
    while True:
        print("\nBookstore Console")
        print("1. Add a book")
        print("2. List books")
        print("3. Modify book")
        print("4. Delete book")
        print("5. Exit")
        try:
            choice = int(input("Enter your choice: "))
            match choice:
                case 1:
                    title = input("Enter the book title: ")
                    author = input("Enter the author's name: ")
                    isbn = input("Enter the ISBN: ")
                    bookstore.add_book(title, author, isbn)
                    print("Book added successfully!")
                case 2:
                    if not bookstore.books:
                        print("No books in the store yet.")
                    else:
                        print("List of books:")
                        bookstore.list_books()
                case 3:
                    bookstore.list_books()
                    numBook=int(input("Which book do you want to modify: "))
                    if (bookstore.existBook(numBook)):
                        newName= input("Name of the new book: ")
                        bookstore.modifyBook(numBook,newName)
                    else:
                        print(f"The book with id {numBook} does not exist")
                case 4:
                    bookstore.list_books()
                    numBook=int(input("Which book do you want to delete: "))
                    if (bookstore.existBook(numBook)):
                        bookstore.deleteBook(numBook)
                    else:
                        print(f"The book with id {numBook} does not exist")
                case 5:
                    print("Goodbye!")
                    break
                case _:
                    print("Invalid choice. Please try again.")
        except ValueError:
            print("Invalid choice. Please try again.")

if __name__ == "__main__":
    main()