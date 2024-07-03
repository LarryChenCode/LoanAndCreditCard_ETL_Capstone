from functions import *

connection = mysql.connector.connect(
    host="localhost",
    user=my_secrets.mysql_username,
    password=my_secrets.mysql_password,
    database="creditcard_capstone"
)

while True:
    print("\nMenu:")
    print("1. Check account details")
    print("2. Modify account details")
    print("3. Generate monthly bill")
    print("4. Display transactions between dates")
    print("5. Exit")
    choice = input("Enter your choice (1-5): ")

    if choice == '1':
        customer_ssn = get_customer_ssn()
        account_details = check_account_details(connection, customer_ssn)

        if account_details:
            account_df = pd.DataFrame(account_details, index=[0]).T
            account_df.columns = ['Value']
            print("Account details:")
            print(account_df)

    elif choice == '2':
        customer_ssn = get_customer_ssn()
        modify_account_details(connection, customer_ssn)

    elif choice == '3':
        card_number = get_credit_card_number()
        month_year = get_month_year()
        bill_amount = generate_monthly_bill(connection, card_number, month_year)
        print(f"Total bill amount for {month_year}: ${bill_amount:.2f}")

        billing_info = {
            'Credit Card Number': [card_number],
            'Month-Year': [month_year],
            'Total Bill Amount': [bill_amount]
        }
        billing_df = pd.DataFrame(billing_info)
        output_dir = "billing_report_cardnumber_monthyear"
        os.makedirs(output_dir, exist_ok=True)
        csv_filename = os.path.join(output_dir, f"billing_{card_number}_{month_year.replace('-', '')}.csv")
        billing_df.to_csv(csv_filename, index=False)
        print(f"Billing information exported to {csv_filename}")

    elif choice == '4':
        customer_ssn = get_customer_ssn()
        start_date = get_date("Enter start date (YYYY-MM-DD): ")
        end_date = get_date("Enter end date (YYYY-MM-DD): ")
        transactions = display_transactions_between_dates(connection, customer_ssn, start_date, end_date)
        if transactions:
            df = pd.DataFrame(transactions)
            print(df)
            output_dir = "transactions_report_ssn_startdate_to_enddate"
            os.makedirs(output_dir, exist_ok=True)
            csv_filename = os.path.join(output_dir, f"transactions_{customer_ssn}_{start_date}_to_{end_date}.csv")
            df.to_csv(csv_filename, index=False)
            print(f"Transactions exported to {csv_filename}")
        else:
            print("No transactions found for the specified date range.")

    elif choice == '5':
        connection.close()
        print("Exiting program.")
        break

    else:
        print("Invalid choice. Please enter a number between 1 and 5.")