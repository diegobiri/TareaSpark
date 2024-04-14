def main():
    while True:
        try:
            num = int(input("Enter a natural number: "))
            contador=0
            if num <= 0:
                print("The number must be positive")
                continue
            
            for i in range(1, num+1):
                if num % i ==0:
                    contador += 1 
            if contador > 2:
                print ("The number is not prime")  
            else:
                print ("The number is prime")
        except ValueError:
            print("Please enter valid natural numbers.")

        another = input("Do you want to check another number (y/n)? ").strip().lower()
        if another != 'y':
            break

if __name__ == "__main__":
    main()
