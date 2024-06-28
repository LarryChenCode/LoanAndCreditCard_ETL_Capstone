from functions import *

connection = mysql.connector.connect(
    host="localhost",
    user=my_secrets.mysql_username,
    password=my_secrets.mysql_password,
    database="creditcard_capstone"
)

# 2.2.1: Check account details
customer_ssn = get_customer_ssn()
account_details = check_account_details(connection, customer_ssn)
print("Account details:", account_details)

if account_details:
    account_df = pd.DataFrame(account_details, index=[0]).T
    account_df.columns = ['Value']
    print(account_df)
        
# 2.2.2: Modify account details
modify_account_details(connection, customer_ssn)

# 2.2.3: Generate monthly bill
card_number = get_credit_card_number()
month_year = get_month_year()
bill_amount = generate_monthly_bill(connection, card_number, month_year)
print(f"Total bill amount for {month_year}: ${bill_amount:.2f}")

# Export billing information to a CSV file
billing_info = {
    'Credit Card Number': [card_number],
    'Month-Year': [month_year],
    'Total Bill Amount': [bill_amount]
}
billing_df = pd.DataFrame(billing_info)

# Create directory if it doesn't exist
output_dir = "billing_report_cardnumber_monthyear"
os.makedirs(output_dir, exist_ok=True)

csv_filename = os.path.join(output_dir, f"billing_{card_number}_{month_year.replace('-', '')}.csv")
billing_df.to_csv(csv_filename, index=False)
print(f"Billing information exported to {csv_filename}")

# 2.2.4: Display transactions between dates
start_date = get_date("Enter start date (YYYY-MM-DD): ")
end_date = get_date("Enter end date (YYYY-MM-DD): ")
transactions = display_transactions_between_dates(connection, customer_ssn, start_date, end_date)

if transactions:
    df = pd.DataFrame(transactions)
    print(df)
    
    # Create directory if it doesn't exist
    output_dir = "transactions_report_ssn_startdate_to_enddate"
    os.makedirs(output_dir, exist_ok=True)
    
    csv_filename = os.path.join(output_dir, f"transactions_{customer_ssn}_{start_date}_to_{end_date}.csv")
    df.to_csv(csv_filename, index=False)
    print(f"Transactions exported to {csv_filename}")
else:
    print("No transactions found for the specified date range.")

connection.close()