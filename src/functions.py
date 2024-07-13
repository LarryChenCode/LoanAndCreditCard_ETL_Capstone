import mysql.connector
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, TimestampType
from pyspark.sql.functions import col, expr, concat_ws, lpad, regexp_replace
import json
import my_secrets
import re
from datetime import datetime
import pandas as pd
import os
import matplotlib.pyplot as plt
import seaborn as sns
import requests
import subprocess
import os
import re
from fpdf import FPDF


# 1. Functional Requirements - Load Credit Card Database (SQL)
# Create a connection to the MySQL database
def create_database(connection, database_name):
    cursor = connection.cursor()
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database_name}")
    cursor.close()
    print(f"Database '{database_name}' created")

# Define the connection to the MySQL database
def define_connection():
    connection = mysql.connector.connect(
        host="localhost",
        user=my_secrets.mysql_username,
        password=my_secrets.mysql_password
    )
    print("Connected to MySQL")
    return connection
    
# Define the JDBC URL and connection properties
def define_jdbc_n_properties():
    jdbc_url = "jdbc:mysql://localhost:3306/creditcard_capstone"
    connection_properties = {
        "user": my_secrets.mysql_username,
        "password": my_secrets.mysql_password,
        "driver": "com.mysql.cj.jdbc.Driver"
    }
    print("jdbc_url and connection_properties defined")
    return jdbc_url, connection_properties

# Initialize spark session
def initialize_spark_session(app_name):
    jdbc_driver_path = "C:\\Spark\\mysql-connector-j-9.0.0.jar"
    spark = SparkSession.builder.master("local[1]").appName(app_name).config("spark.driver.extraClassPath", jdbc_driver_path).getOrCreate()
    print("Spark session initialized")
    return spark

# Define the schema for the JSON files
def get_schema():
    branch_schema = StructType([
        StructField("BRANCH_CODE", IntegerType(), True),
        StructField("BRANCH_NAME", StringType(), True),
        StructField("BRANCH_STREET", StringType(), True),
        StructField("BRANCH_CITY", StringType(), True),
        StructField("BRANCH_STATE", StringType(), True),
        StructField("BRANCH_ZIP", StringType(), True),
        StructField("BRANCH_PHONE", StringType(), True),
        StructField("LAST_UPDATED", TimestampType(), True)
    ])

    credit_schema = StructType([
        StructField("CREDIT_CARD_NO", StringType(), True),
        StructField("DAY", IntegerType(), True),
        StructField("MONTH", IntegerType(), True),
        StructField("YEAR", IntegerType(), True),
        StructField("CUST_SSN", IntegerType(), True),
        StructField("BRANCH_CODE", IntegerType(), True),
        StructField("TRANSACTION_TYPE", StringType(), True),
        StructField("TRANSACTION_VALUE", DoubleType(), True),
        StructField("TRANSACTION_ID", IntegerType(), True)
    ])

    customer_schema = StructType([
        StructField("SSN", IntegerType(), True),
        StructField("FIRST_NAME", StringType(), True),
        StructField("MIDDLE_NAME", StringType(), True),
        StructField("LAST_NAME", StringType(), True),
        StructField("CREDIT_CARD_NO", StringType(), True),
        StructField("APT_NO", StringType(), True),
        StructField("STREET_NAME", StringType(), True),
        StructField("CUST_CITY", StringType(), True),
        StructField("CUST_STATE", StringType(), True),
        StructField("CUST_COUNTRY", StringType(), True),
        StructField("CUST_ZIP", StringType(), True),
        StructField("CUST_PHONE", StringType(), True),
        StructField("CUST_EMAIL", StringType(), True),
        StructField("LAST_UPDATED", TimestampType(), True)
    ])

    return branch_schema, credit_schema, customer_schema

# Read JSON files into Spark DataFrames
def read_json_to_df(spark, schema, file_path):
    return spark.read.schema(schema).option("multiline", "true").json(file_path)

# Load data into MySQL database
def transform_and_load_branch_data(branch_df, jdbc_url, connection_properties):
    branch_transformed_df = branch_df.withColumn("BRANCH_ZIP", lpad(col("BRANCH_ZIP").cast("string"), 5, "0")) \
        .withColumn("BRANCH_PHONE", expr("concat('(', substring(BRANCH_PHONE, 1, 3), ')', substring(BRANCH_PHONE, 4, 3), '-', substring(BRANCH_PHONE, 7, 4))"))
    branch_transformed_df.write.jdbc(url=jdbc_url, table="CDW_SAPP_BRANCH", mode="overwrite", properties=connection_properties)
    print("CDW_SAPP_BRANCH table loaded successfully")

# Transform and load credit card data
def transform_and_load_credit_card_data(credit_card_df, jdbc_url, connection_properties):
    credit_card_transformed_df = credit_card_df.withColumn("TIMEID", concat_ws("", col("YEAR"), lpad(col("MONTH").cast("string"), 2, "0"), lpad(col("DAY").cast("string"), 2, "0")))
    credit_card_transformed_df = credit_card_transformed_df.drop("DAY", "MONTH", "YEAR")
    credit_card_transformed_df.write.jdbc(url=jdbc_url, table="CDW_SAPP_CREDIT_CARD", mode="overwrite", properties=connection_properties)
    print("CDW_SAPP_CREDIT_CARD table loaded successfully")

# Transform and load customer data
def transform_and_load_customer_data(customer_df, jdbc_url, connection_properties):
    customer_transformed_df = customer_df.withColumn("FIRST_NAME", expr("initcap(FIRST_NAME)")) \
        .withColumn("MIDDLE_NAME", expr("lower(MIDDLE_NAME)")) \
        .withColumn("LAST_NAME", expr("initcap(LAST_NAME)")) \
        .withColumn("FULL_STREET_ADDRESS", concat_ws(" ", col("APT_NO"), col("STREET_NAME"))) \
        .withColumn("CUST_PHONE", expr("concat('614', CUST_PHONE)")) \
        .withColumn("CUST_PHONE", expr("concat('(', substring(CUST_PHONE, 1, 3), ')', substring(CUST_PHONE, 4, 3), '-', substring(CUST_PHONE, 7, 4))")) \
        .withColumn("CUST_CITY", regexp_replace(col("CUST_CITY"), r"(?<=[a-z])(?=[A-Z])", " "))
    customer_transformed_df = customer_transformed_df.drop("APT_NO", "STREET_NAME")
    customer_transformed_df.write.jdbc(url=jdbc_url, table="CDW_SAPP_CUSTOMER", mode="overwrite", properties=connection_properties)
    print("CDW_SAPP_CUSTOMER table loaded successfully")

# Load Json file
def load_json(file_path):
    with open(file_path, "r") as file:
        return json.load(file)

# Check the number of rows in a table
def check_row_count(cursor, table_name, expected_count):
    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    result = cursor.fetchone()
    if expected_count != result[0]:
        print(f"Number of rows in {table_name} table is different from the JSON file. Expected: {expected_count}, Found: {result[0]}")
        return False
    return True

# Check if the number of rows in the JSON files and the MySQL database are the same
def check_json_files_with_database(connection, branch_json, credit_card_json, customer_json):
    
    cursor = connection.cursor()
    cursor.execute("USE creditcard_capstone")
    
    branch_correct = check_row_count(cursor, "CDW_SAPP_BRANCH", len(branch_json))
    credit_card_correct = check_row_count(cursor, "CDW_SAPP_CREDIT_CARD", len(credit_card_json))
    customer_correct = check_row_count(cursor, "CDW_SAPP_CUSTOMER", len(customer_json))
    
    if branch_correct and credit_card_correct and customer_correct:
        print("Data load correct: All tables have the same number of rows as the JSON files.")

    cursor.close()
    connection.close()
        

# 2. Functional Requirements - Application Front-End
# 2.1 Transaction Details Module
# Get the zip code from the user
def get_zip_code():
    while True:
        zip_code = input("Please enter a 5-digit zip code: ")
        if re.match(r'^\d{5}$', zip_code):
            return zip_code
        else:
            print("Invalid zip code format. Please enter exactly 5 digits.")

# Get the month and year in MM-YYYY format
def get_month_year():
    while True:
        month_year = input("Please enter a month and year in MM-YYYY format: ")
        try:
            datetime.strptime(month_year, "%m-%Y")
            return month_year
        except ValueError:
            print("Invalid format. Please enter in MM-YYYY format.")

# Query transactions based on zip code and month-year
def query_transactions(connection, zip_code, month_year):
    month, year = month_year.split('-')
    month_year_prefix = f"{year}{month.zfill(2)}"
    
    cursor = connection.cursor(dictionary=True)
    
    query = """
    SELECT * FROM CDW_SAPP_CREDIT_CARD
    WHERE BRANCH_CODE IN (
        SELECT BRANCH_CODE FROM CDW_SAPP_BRANCH WHERE BRANCH_ZIP = %s
    ) AND TIMEID LIKE %s
    ORDER BY TIMEID DESC
    """
    
    cursor.execute(query, (zip_code, f"{month_year_prefix}%"))
    transactions = cursor.fetchall()
    cursor.close()
    
    if not transactions:
        print("No transactions found for the specified zip code and date range.")
        input("Press any key to continue...")
        return None
    
    return transactions

# Display transactions in a DataFrame
def display_transactions(transactions):
    if transactions:
        df = pd.DataFrame(transactions)
        print(df)
        return df
    else:
        return None


# 2.2 Customer Details Module
# Check the existing account details of a customer
def check_account_details(connection, customer_ssn):
    cursor = connection.cursor(dictionary=True)
    cursor.execute("SELECT * FROM CDW_SAPP_CUSTOMER WHERE SSN = %s", (customer_ssn,))
    account_details = cursor.fetchone()
    cursor.close()
    return account_details

# Get the customer's SSN
def get_customer_ssn():
    while True:
        ssn = input("Please enter the customer's SSN: ")
        print('\n')
        if re.match(r'^\d{9}$', ssn):
            return ssn
        else:
            print("Invalid SSN format. Please enter exactly 9 digits.")

# # Modify the existing account details of a customer
# def modify_account_details(connection, customer_ssn):
#     cursor = connection.cursor(dictionary=True)
#     cursor.execute("SELECT * FROM CDW_SAPP_CUSTOMER WHERE SSN = %s", (customer_ssn,))
#     account_details = cursor.fetchone()
#     cursor.close()

#     if not account_details:
#         print("Customer not found.")
#         input("Press any key to continue...")
#         return

#     fields = ['FIRST_NAME', 'MIDDLE_NAME', 'LAST_NAME', 'FULL_STREET_ADDRESS', 'CUST_CITY', 'CUST_STATE', 'CUST_COUNTRY', 'CUST_ZIP', 'CUST_PHONE', 'CUST_EMAIL']
#     new_details = {}

#     for field in fields:
#         update = input(f"Do you want to update {field} (current value: {account_details[field]})? (y/n): ")
#         if update.lower() == 'y':
#             new_value = input(f"Enter new value for {field}: ")
#             if field == 'CUST_PHONE':
#                 while not validate_phone_number(new_value):
#                     new_value = input("Invalid phone number format. Please enter a 10-digit phone number: ")
#                 new_value = format_phone_number(new_value)
#             elif field == 'CUST_CITY':
#                 new_value = format_city_name(new_value)
#             elif field == 'CUST_ZIP':
#                 while not validate_zip_code(new_value):
#                     new_value = input("Invalid zip code format. Please enter a 5-digit zip code: ")
#             elif field == 'CUST_EMAIL':
#                 while not validate_email(new_value):
#                     new_value = input("Invalid email format. Please enter an email in the format xxxx@xxx.xxx: ")
#             new_details[field] = new_value
#         else:
#             new_details[field] = account_details[field]

#     print("\nThe following information will be updated:")
#     for field, value in new_details.items():
#         print(f"{field}: {value}")
    
#     confirm = input("\nIs the above information correct? (yes/no): ").strip().lower()
#     if confirm == 'yes':
#         cursor = connection.cursor()
#         update_query = """
#         UPDATE CDW_SAPP_CUSTOMER
#         SET FIRST_NAME = %s, MIDDLE_NAME = %s, LAST_NAME = %s, 
#             FULL_STREET_ADDRESS = %s, CUST_CITY = %s, 
#             CUST_STATE = %s, CUST_COUNTRY = %s, CUST_ZIP = %s, 
#             CUST_PHONE = %s, CUST_EMAIL = %s, LAST_UPDATED = NOW()
#         WHERE SSN = %s
#         """
#         cursor.execute(update_query, (
#             new_details['FIRST_NAME'], new_details['MIDDLE_NAME'], new_details['LAST_NAME'],
#             new_details['FULL_STREET_ADDRESS'], new_details['CUST_CITY'],
#             new_details['CUST_STATE'], new_details['CUST_COUNTRY'], new_details['CUST_ZIP'],
#             new_details['CUST_PHONE'], new_details['CUST_EMAIL'],
#             customer_ssn
#         ))
#         connection.commit()
#         cursor.close()

#         print("Account details updated.")
#     else:
#         print("Please run the update process again to correct the information.")

#     input("Press any key to continue...")

# def format_phone_number(phone_number):
#     # Remove any non-digit characters
#     digits = re.sub(r'\D', '', phone_number)
#     # Format the digits as (XXX)XXX-XXXX
#     formatted_number = f"({digits[:3]}){digits[3:6]}-{digits[6:]}"
#     return formatted_number

# def validate_phone_number(phone_number):
#     digits = re.sub(r'\D', '', phone_number)
#     return len(digits) == 10

# def validate_zip_code(zip_code):
#     return re.match(r'^\d{5}$', zip_code) is not None

# def validate_email(email):
#     return re.match(r'^[\w\.-]+@[\w\.-]+\.\w+$', email) is not None

# def format_city_name(city_name):
#     # Insert a space before each capital letter in the middle of the word
#     formatted_city = re.sub(r'(?<=[a-z])(?=[A-Z])', ' ', city_name)
#     return formatted_city



# Modify the existing account details of a customer
def modify_account_details(connection, customer_ssn):
    cursor = connection.cursor(dictionary=True)
    cursor.execute("SELECT * FROM CDW_SAPP_CUSTOMER WHERE SSN = %s", (customer_ssn,))
    account_details = cursor.fetchone()
    cursor.close()

    if not account_details:
        print("Customer not found.")
        input("Press any key to continue...")
        return

    fields = ['FIRST_NAME', 'MIDDLE_NAME', 'LAST_NAME', 'FULL_STREET_ADDRESS', 'CUST_CITY', 'CUST_STATE', 'CUST_COUNTRY', 'CUST_ZIP', 'CUST_PHONE', 'CUST_EMAIL']

    while True:
        new_details = account_details.copy()
        while True:
            print("\nSelect the field you want to update:")
            for idx, field in enumerate(fields, 1):
                print(f"{idx}. {field} (current value: {account_details[field]})")

            choice = input("\nEnter the number of the field you want to update (or 'done' to finish): ").strip().lower()
            if choice == 'done':
                break

            if not choice.isdigit() or int(choice) < 1 or int(choice) > len(fields):
                print("Invalid choice. Please enter a valid number.")
                continue

            field = fields[int(choice) - 1]
            new_value = input(f"Enter new value for {field}: ")

            if field == 'CUST_PHONE':
                while not validate_phone_number(new_value):
                    new_value = input("Invalid phone number format. Please enter a 10-digit phone number: ")
                new_value = format_phone_number(new_value)
            elif field == 'CUST_CITY':
                new_value = format_city_name(new_value)
            elif field == 'CUST_ZIP':
                while not validate_zip_code(new_value):
                    new_value = input("Invalid zip code format. Please enter a 5-digit zip code: ")
            elif field == 'CUST_EMAIL':
                while not validate_email(new_value):
                    new_value = input("Invalid email format. Please enter an email in the format xxxx@xxx.xxx: ")

            new_details[field] = new_value

        print("\nThe following information will be updated:")
        for field, value in new_details.items():
            print(f"{field}: {value}")

        confirm = input("\nIs the above information correct? (yes/no): ").strip().lower()
        if confirm == 'yes':
            cursor = connection.cursor()
            update_query = """
            UPDATE CDW_SAPP_CUSTOMER
            SET FIRST_NAME = %s, MIDDLE_NAME = %s, LAST_NAME = %s, 
                FULL_STREET_ADDRESS = %s, CUST_CITY = %s, 
                CUST_STATE = %s, CUST_COUNTRY = %s, CUST_ZIP = %s, 
                CUST_PHONE = %s, CUST_EMAIL = %s, LAST_UPDATED = NOW()
            WHERE SSN = %s
            """
            cursor.execute(update_query, (
                new_details['FIRST_NAME'], new_details['MIDDLE_NAME'], new_details['LAST_NAME'],
                new_details['FULL_STREET_ADDRESS'], new_details['CUST_CITY'],
                new_details['CUST_STATE'], new_details['CUST_COUNTRY'], new_details['CUST_ZIP'],
                new_details['CUST_PHONE'], new_details['CUST_EMAIL'],
                customer_ssn
            ))
            connection.commit()
            cursor.close()

            print("Account details updated.")
            break
        else:
            print("Please select the fields again to correct the information.")
            input("Press any key to continue...")

    input("Press any key to continue...")

def format_phone_number(phone_number):
    # Remove any non-digit characters
    digits = re.sub(r'\D', '', phone_number)
    # Format the digits as (XXX)XXX-XXXX
    formatted_number = f"({digits[:3]}){digits[3:6]}-{digits[6:]}"
    return formatted_number

def validate_phone_number(phone_number):
    digits = re.sub(r'\D', '', phone_number)
    return len(digits) == 10

def validate_zip_code(zip_code):
    return re.match(r'^\d{5}$', zip_code) is not None

def validate_email(email):
    return re.match(r'^[\w\.-]+@[\w\.-]+\.\w+$', email) is not None

def format_city_name(city_name):
    # Insert a space before each capital letter in the middle of the word
    formatted_city = re.sub(r'(?<=[a-z])(?=[A-Z])', ' ', city_name)
    return formatted_city





# Generate a monthly bill for a credit card
def generate_monthly_bill(connection, card_number, month_year):
    month, year = month_year.split('-')
    month_year_prefix = f"{year}{month.zfill(2)}"
    
    cursor = connection.cursor(dictionary=True)
    
    query = """
    SELECT * 
    FROM CDW_SAPP_CREDIT_CARD
    WHERE CREDIT_CARD_NO = %s AND TIMEID LIKE %s
    ORDER BY TIMEID DESC
    """
    
    cursor.execute(query, (card_number, f"{month_year_prefix}%"))
    transactions = cursor.fetchall()
    cursor.close()
    
    if not transactions:
        print("No transactions found for the specified credit card and date range.")
        input("Press any key to continue...")
        return None
    
    total_amount = sum(transaction['TRANSACTION_VALUE'] for transaction in transactions)
    
    # Create a DataFrame for the transactions
    df = pd.DataFrame(transactions)
    
    # Create PDF
    pdf = FPDF()
    pdf.add_page()
    pdf.set_font("Arial", size=12)

    pdf.cell(200, 10, txt=f"Monthly Bill for Credit Card: {card_number}", ln=True, align='C')
    pdf.cell(200, 10, txt=f"Month-Year: {month_year}", ln=True, align='C')
    pdf.cell(200, 10, txt=f"Total Amount: ${total_amount:.2f}", ln=True, align='C')
    
    pdf.ln(10)
    pdf.cell(200, 10, txt="Transactions:", ln=True, align='L')
    pdf.ln(10)

    # Add table header
    col_width = pdf.w / 4.5
    row_height = pdf.font_size
    pdf.set_font("Arial", 'B', 12)
    pdf.cell(col_width, row_height * 2, "Transaction ID", border=1)
    pdf.cell(col_width, row_height * 2, "Transaction Date", border=1)
    pdf.cell(col_width, row_height * 2, "Transaction Value", border=1)
    pdf.cell(col_width, row_height * 2, "Transaction Type", border=1)
    pdf.ln(row_height * 2)

    # Add table rows
    pdf.set_font("Arial", size=10)
    for transaction in transactions:
        pdf.cell(col_width, row_height * 2, str(transaction['TRANSACTION_ID']), border=1)
        pdf.cell(col_width, row_height * 2, transaction['TIMEID'], border=1)
        pdf.cell(col_width, row_height * 2, f"${transaction['TRANSACTION_VALUE']:.2f}", border=1)
        pdf.cell(col_width, row_height * 2, transaction['TRANSACTION_TYPE'], border=1)
        pdf.ln(row_height * 2)

    # Create directories if they don't exist
    output_dir = "../report/billing_report_cardnumber_monthyear"
    os.makedirs(output_dir, exist_ok=True)

    # Save PDF
    pdf_filename = os.path.join(output_dir, f"billing_{card_number}_{month_year.replace('-', '')}.pdf")
    pdf.output(pdf_filename)
    print(f"Monthly bill exported to {pdf_filename}")

    # Save CSV
    csv_filename = os.path.join(output_dir, f"billing_{card_number}_{month_year.replace('-', '')}.csv")
    df.to_csv(csv_filename, index=False)
    print(f"Monthly bill transactions exported to {csv_filename}")

    return total_amount


# Get the credit card number from the user
def get_credit_card_number():
    while True:
        card_number = input("Please enter the credit card number: ")
        if re.match(r'^\d{16}$', card_number):
            return card_number
        else:
            print("Invalid credit card number format. Please enter exactly 16 digits.")

# Display the transactions made by a customer between two dates, ordered by year, month, and day in descending order
def display_transactions_between_dates(connection, customer_ssn, start_date, end_date):
    cursor = connection.cursor(dictionary=True)
    
    query = """
    SELECT * FROM CDW_SAPP_CREDIT_CARD
    WHERE CUST_SSN = %s AND TIMEID BETWEEN %s AND %s
    ORDER BY TIMEID DESC
    """
    
    start_timeid = start_date.replace("-", "")
    end_timeid = end_date.replace("-", "")
    
    cursor.execute(query, (customer_ssn, start_timeid, end_timeid))
    transactions = cursor.fetchall()
    cursor.close()
    
    if not transactions:
        print("No transactions found for the specified date range.")
        input("Press any key to continue...")
        return None
    
    return transactions

# Get the date from the user
def get_date(prompt):
    while True:
        date_input = input(prompt)
        try:
            datetime.strptime(date_input, "%Y-%m-%d")
            return date_input
        except ValueError:
            print("Invalid date format. Please enter in YYYY-MM-DD format.")

# 2.3 Create a Main Menu
# Define the connection to the MySQL database
def define_connection_with_db():
    connection = mysql.connector.connect(
        host="localhost",
        user=my_secrets.mysql_username,
        password=my_secrets.mysql_password,
        database="creditcard_capstone"  # Ensure the correct database is selected
    )
    print("Connected to MySQL")
    return connection

# Create transaction details module
def transaction_details_module(connection):
    first_time = True
    while True:
        if not first_time:
            continue_choice = input("Would you like to continue? (y/n): ").strip().lower()
            if continue_choice == 'n':
                print("Returning to main menu...")
                break
            elif continue_choice != 'y':
                print("Invalid choice. Please enter 'y' for yes or 'n' for no. (only lower characters)")
                continue

        zip_code = get_zip_code()
        if zip_code is None:
            return  # Exit to main menu if 'exit' is entered
        month_year = get_month_year()
        if month_year is None:
            return  # Exit to main menu if 'exit' is entered
        transactions = query_transactions(connection, zip_code, month_year)
        df = display_transactions(transactions)

        if df is not None:
            output_dir = "../report/transactions_report_zipcode_MMYYYY"
            os.makedirs(output_dir, exist_ok=True)
            
            csv_filename = os.path.join(output_dir, f"transactions_{zip_code}_{month_year.replace('-', '')}.csv")
            df.to_csv(csv_filename, index=False)
            print(f"Transactions exported to {csv_filename}")
        else:
            print("No transactions found for the specified zip code and date range.")
        
        first_time = False  # From now on, ask if the user wants to continue


# Create customer details module
def customer_details_module(connection):
    while True:
        print("\nCustomer Details Module:")
        print("> 1. Check account details")
        print("> 2. Modify account details")
        print("> 3. Generate monthly bill")
        print("> 4. Display transactions between dates")
        print("> 5. Exit")
        choice = input("Enter your choice (1-5): ")

        if choice == '1':
            customer_ssn = get_customer_ssn()
            account_details = check_account_details(connection, customer_ssn)

            if account_details:
                account_df = pd.DataFrame(account_details, index=[0]).T
                account_df.columns = ['Value']
                print("Account details:")
                print(account_df)
            else:
                print("No account details found. Please enter another SSN or check the SSN.")

        elif choice == '2':
            customer_ssn = get_customer_ssn()
            modify_account_details(connection, customer_ssn)

        elif choice == '3':
            card_number = get_credit_card_number()
            month_year = get_month_year()
            bill_amount = generate_monthly_bill(connection, card_number, month_year)
            if bill_amount is not None:
                print(f"Total bill amount for {month_year}: ${bill_amount:.2f}")

                billing_info = {
                    'Credit Card Number': [card_number],
                    'Month-Year': [month_year],
                    'Total Bill Amount': [bill_amount]
                }
                billing_df = pd.DataFrame(billing_info)
                output_dir = "../report/billing_report_cardnumber_monthyear"
                os.makedirs(output_dir, exist_ok=True)
                csv_filename = os.path.join(output_dir, f"billing_{card_number}_{month_year.replace('-', '')}.csv")
                billing_df.to_csv(csv_filename, index=False)
                print(f"Billing information exported to {csv_filename}")
            else:
                print("Please enter another credit card number or month-year combination.")

        elif choice == '4':
            customer_ssn = get_customer_ssn()
            start_date = get_date("Enter start date (YYYY-MM-DD): ")
            end_date = get_date("Enter end date (YYYY-MM-DD): ")
            transactions = display_transactions_between_dates(connection, customer_ssn, start_date, end_date)
            if transactions:
                df = pd.DataFrame(transactions)
                print(df)
                output_dir = "../report/transactions_report_ssn_startdate_to_enddate"
                os.makedirs(output_dir, exist_ok=True)
                csv_filename = os.path.join(output_dir, f"transactions_{customer_ssn}_{start_date}_to_{end_date}.csv")
                df.to_csv(csv_filename, index=False)
                print(f"Transactions exported to {csv_filename}")
            else:
                print("No transactions found for the specified date range.")

        elif choice == '5':
            print("Exiting Customer Details Module.")
            break

        else:
            print("Invalid choice. Please enter a number between 1 and 5.")


# 3. Functional Requirements - Data Analysis and Visualization
# Plot the transaction count by transaction type
def plot_transaction_type_count(connection):
    query = """
    SELECT TRANSACTION_TYPE, COUNT(*) AS transaction_count
    FROM CDW_SAPP_CREDIT_CARD
    GROUP BY TRANSACTION_TYPE
    ORDER BY transaction_count DESC
    """
    df = pd.read_sql(query, connection)

    plt.figure(figsize=(10, 6))
    sns.barplot(x='TRANSACTION_TYPE', y='transaction_count', data=df, palette='viridis')
    plt.title('Transaction Count by Transaction Type')
    plt.xlabel('Transaction Type')
    plt.ylabel('Transaction Count')
    plt.xticks(rotation=45)
    plt.tight_layout()
    
    output_dir = "visualizations"
    os.makedirs(output_dir, exist_ok=True)
    filename = os.path.join(output_dir, "transaction_type_count.png")
    plt.savefig(filename)
    plt.show()
    print(f"Visualization saved as {filename}")
    
# Plot the total transaction value by branch
def plot_top_states_with_customers(connection):
    query = """
    SELECT CUST_STATE, COUNT(*) AS customer_count
    FROM CDW_SAPP_CUSTOMER
    GROUP BY CUST_STATE
    ORDER BY customer_count DESC
    LIMIT 10
    """
    df = pd.read_sql(query, connection)

    plt.figure(figsize=(10, 6))
    sns.barplot(x='CUST_STATE', y='customer_count', data=df, palette='viridis')
    plt.title('Top 10 States with the Highest Number of Customers')
    plt.xlabel('State')
    plt.ylabel('Customer Count')
    plt.xticks(rotation=45)
    plt.tight_layout()
    
    output_dir = "visualizations"
    os.makedirs(output_dir, exist_ok=True)
    filename = os.path.join(output_dir, "top_10_states_customers.png")
    plt.savefig(filename)
    plt.show()
    print(f"Visualization saved as {filename}")

# Plot the top 10 customers by transaction sum
def plot_top_customers_by_transaction_sum(connection):
    query = """
    SELECT CUST_SSN, SUM(TRANSACTION_VALUE) AS total_transaction_sum
    FROM CDW_SAPP_CREDIT_CARD
    GROUP BY CUST_SSN
    ORDER BY total_transaction_sum DESC
    LIMIT 10
    """
    df = pd.read_sql(query, connection)

    plt.figure(figsize=(10, 6))
    sns.barplot(x='CUST_SSN', y='total_transaction_sum', data=df, palette='viridis')
    plt.title('Top 10 Customers by Transaction Sum')
    plt.xlabel('Customer SSN')
    plt.ylabel('Total Transaction Sum ($)')
    plt.xticks(rotation=45)
    plt.tight_layout()
    
    output_dir = "visualizations"
    os.makedirs(output_dir, exist_ok=True)
    filename = os.path.join(output_dir, "top_10_customers_transaction_sum.png")
    plt.savefig(filename)
    plt.show()
    print(f"Visualization saved as {filename}")


# 4. Functional Requirements - LOAN Application Dataset
# Get the loan application data from the API
def get_loan_data(api_url):
    response = requests.get(api_url)
    status_code = response.status_code
    print(f"Status code of the API endpoint: {status_code}")
    if status_code == 200:
        loan_data = response.json()
        return loan_data
    else:
        print(f"Failed to fetch data. Status code: {status_code}")
        return None

# Load the loan application data into the RDBMS
def load_loan_data_to_rdbms(spark, loan_data, jdbc_url, connection_properties):
    loan_schema = StructType([
        StructField("Application_ID", StringType(), True),
        StructField("Gender", StringType(), True),
        StructField("Married", StringType(), True),
        StructField("Dependents", StringType(), True),
        StructField("Education", StringType(), True),
        StructField("Self_Employed", StringType(), True),
        StructField("Credit_History", IntegerType(), True),
        StructField("Property_Area", StringType(), True),
        StructField("Income", StringType(), True),
        StructField("Application_Status", StringType(), True)
    ])
    
    loan_df = spark.createDataFrame(loan_data, schema=loan_schema)
    loan_df.write.jdbc(url=jdbc_url, table="CDW_SAPP_loan_application", mode="overwrite", properties=connection_properties)
    print("Loan data loaded into RDBMS")

# Display the loan application data
def display_loan_data(loan_data):
    loan_df = pd.DataFrame(loan_data)
    print("\nLoan Data:")
    print(loan_df)
    

# 5. Functional Requirements - Data Analysis and Visualization for Loan Application
# Plot the approval percentage of self-employed applicants
def plot_self_employed_approval_percentage(connection):
    query = """
    SELECT 
           COUNT(*) AS total, 
           SUM(CASE WHEN Application_Status = 'Y' THEN 1 ELSE 0 END) AS approved
    FROM CDW_SAPP_loan_application
    WHERE Self_Employed = 'Yes'
    """
    df = pd.read_sql(query, connection)
    
    # Calculate the approval percentage
    df['approval_percentage'] = (df['approved'] / df['total']) * 100

    # Print the counts
    print("Self-Employed Applicants (Yes) Counts:")
    print(f"Total Applications: {df['total'][0]}")
    print(f"Approved Applications: {df['approved'][0]}")
    print(f"Approval Percentage: {df['approval_percentage'][0]:.2f}%")
    print()

    # Create a DataFrame for plotting
    plot_df = pd.DataFrame({
        'Self_Employed': ['Yes'],
        'approval_percentage': df['approval_percentage']
    })

    plt.figure(figsize=(8, 6))
    sns.barplot(x='Self_Employed', y='approval_percentage', data=plot_df, palette='viridis')
    plt.title('Approval Percentage for Self-Employed Applicants')
    plt.xlabel('Self-Employed')
    plt.ylabel('Approval Percentage')
    plt.ylim(0, 100)  # Set y-axis limit to 100%
    plt.tight_layout()

    output_dir = "visualizations"
    os.makedirs(output_dir, exist_ok=True)
    filename = os.path.join(output_dir, "self_employed_approval_percentage.png")
    plt.savefig(filename)
    plt.show()
    print(f"Visualization saved as {filename}")

# Plot the rejection percentage for married male applicants
def plot_married_male_rejection_percentage(connection):
    query = """
    SELECT Gender, Married, COUNT(*) AS total, 
           SUM(CASE WHEN Application_Status = 'N' THEN 1 ELSE 0 END) AS rejected
    FROM CDW_SAPP_loan_application
    WHERE Gender = 'Male' AND Married = 'Yes'
    GROUP BY Gender, Married
    """
    df = pd.read_sql(query, connection)
    df['rejection_percentage'] = (df['rejected'] / df['total']) * 100

    plt.figure(figsize=(8, 6))
    sns.barplot(x='Married', y='rejection_percentage', data=df, palette='viridis')
    plt.title('Rejection Percentage for Married Male Applicants')
    plt.xlabel('Married')
    plt.ylabel('Rejection Percentage')
    plt.tight_layout()

    output_dir = "visualizations"
    os.makedirs(output_dir, exist_ok=True)
    filename = os.path.join(output_dir, "married_male_rejection_percentage.png")
    plt.savefig(filename)
    plt.show()
    print(f"Visualization saved as {filename}")

# Plot top three property areas with the highest transactions count
def plot_top_three_months_transaction_volume(connection):
    query = """
    SELECT SUBSTRING(TIMEID, 1, 6) AS YearMonth, COUNT(*) AS transaction_count
    FROM CDW_SAPP_CREDIT_CARD
    GROUP BY YearMonth
    ORDER BY transaction_count DESC
    LIMIT 3
    """
    df = pd.read_sql(query, connection)

    # Convert YearMonth to readable format (YYYY-MM)
    df['YearMonth'] = df['YearMonth'].apply(lambda x: f"{x[:4]}-{x[4:6]}")

    plt.figure(figsize=(8, 6))
    sns.barplot(x='YearMonth', y='transaction_count', data=df, palette='viridis')
    plt.title('Top 3 Months with Largest Transaction Volume')
    plt.xlabel('Year-Month')
    plt.ylabel('Transaction Volume')
    plt.tight_layout()

    output_dir = "visualizations"
    os.makedirs(output_dir, exist_ok=True)
    filename = os.path.join(output_dir, "top_three_months_transaction_volume.png")
    plt.savefig(filename)
    plt.show()
    print(f"Visualization saved as {filename}")

# Plot the highest total value of healthcare transactions by branch
def plot_highest_healthcare_transactions_branch(connection):
    query = """
    SELECT BRANCH_CODE, SUM(TRANSACTION_VALUE) AS total_value
    FROM CDW_SAPP_CREDIT_CARD
    WHERE TRANSACTION_TYPE = 'Healthcare'
    GROUP BY BRANCH_CODE
    ORDER BY total_value DESC
    LIMIT 1
    """
    df = pd.read_sql(query, connection)

    plt.figure(figsize=(8, 6))
    sns.barplot(x='BRANCH_CODE', y='total_value', data=df, palette='viridis')
    plt.title('Branch with Highest Total Value of Healthcare Transactions')
    plt.xlabel('Branch Code')
    plt.ylabel('Total Transaction Value ($)')
    plt.tight_layout()

    output_dir = "visualizations"
    os.makedirs(output_dir, exist_ok=True)
    filename = os.path.join(output_dir, "highest_healthcare_transactions_branch.png")
    plt.savefig(filename)
    plt.show()
    print(f"Visualization saved as {filename}")