from functions import *

connection = mysql.connector.connect(
    host="localhost",
    user=my_secrets.mysql_username,
    password=my_secrets.mysql_password,
    database="creditcard_capstone"
)

zip_code = get_zip_code()
month_year = get_month_year()
transactions = query_transactions(connection, zip_code, month_year)
df = display_transactions(transactions)

if df is not None:
    # Create directory if it doesn't exist
    output_dir = "transactions_report_zipcode_MMYYYY"
    os.makedirs(output_dir, exist_ok=True)
    
    csv_filename = os.path.join(output_dir, f"transactions_{zip_code}_{month_year.replace('-', '')}.csv")
    df.to_csv(csv_filename, index=False)
    print(f"Transactions exported to {csv_filename}")

connection.close()
