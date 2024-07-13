from functions import *

connection = define_connection_with_db()

print("Welcome to the Credit Card System!!!")

while True:
    print("\nMain Menu:")
    print("> 1. Search Transaction by Zip Code and Month")
    print("> 2. Customer Information System")
    print("> 3. Exit")
    choice = input("Enter your choice (1-3): ")

    if choice == '1':
        transaction_details_module(connection)

    elif choice == '2':
        customer_details_module(connection)

    elif choice == '3':
        connection.close()
        print("Exiting program.")
        break

    else:
        print("Invalid choice. Please enter a number between 1 and 3.")
        
connection.close()