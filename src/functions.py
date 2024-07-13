import mysql.connector
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, TimestampType
from pyspark.sql.functions import col, expr, concat_ws, lpad
import json
import my_secrets
import re
from datetime import datetime
import pandas as pd
import os
import matplotlib.pyplot as plt
import seaborn as sns
import requests

# 1. Functional Requirements - Load Credit Card Database (SQL)
# Create a connection to the MySQL database
def create_database(connection, database_name):
    cursor = connection.cursor()
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database_name}")
    cursor.close()
    print(f"Database '{database_name}' created")

# Initialize spark session
def initialize_spark_session(app_name):
    jdbc_driver_path = "C:\Spark\mysql-connector-j-9.0.0.jar"
    return SparkSession.builder.master("local[1]").appName(app_name).config("spark.driver.extraClassPath", jdbc_driver_path).getOrCreate()

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

# Transform and load credit card data
def transform_and_load_credit_card_data(credit_card_df, jdbc_url, connection_properties):
    credit_card_transformed_df = credit_card_df.withColumn("TIMEID", concat_ws("", col("YEAR"), lpad(col("MONTH").cast("string"), 2, "0"), lpad(col("DAY").cast("string"), 2, "0")))
    credit_card_transformed_df = credit_card_transformed_df.drop("DAY", "MONTH", "YEAR")
    credit_card_transformed_df.write.jdbc(url=jdbc_url, table="CDW_SAPP_CREDIT_CARD", mode="overwrite", properties=connection_properties)

# Transform and load customer data
def transform_and_load_customer_data(customer_df, jdbc_url, connection_properties):
    customer_transformed_df = customer_df.withColumn("FIRST_NAME", expr("initcap(FIRST_NAME)")) \
        .withColumn("MIDDLE_NAME", expr("lower(MIDDLE_NAME)")) \
        .withColumn("LAST_NAME", expr("initcap(LAST_NAME)")) \
        .withColumn("FULL_STREET_ADDRESS", concat_ws(" ", col("APT_NO"), col("STREET_NAME"))) \
        .withColumn("CUST_PHONE", expr("concat('614', CUST_PHONE)")) \
        .withColumn("CUST_PHONE", expr("concat('(', substring(CUST_PHONE, 1, 3), ')', substring(CUST_PHONE, 4, 3), '-', substring(CUST_PHONE, 7, 4))"))
    customer_transformed_df = customer_transformed_df.drop("APT_NO", "STREET_NAME")
    customer_transformed_df.write.jdbc(url=jdbc_url, table="CDW_SAPP_CUSTOMER", mode="overwrite", properties=connection_properties)

# Load Json file
def load_json(file_path):
    with open(file_path, "r") as file:
        return json.load(file)

# Check the number of rows in a table
def check_row_count(cursor, table_name, expected_count):
    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    result = cursor.fetchone()
    if expected_count == result[0]:
        print(f"Number of rows in {table_name} table is the same as the JSON file")
    else:
        print(f"Number of rows in {table_name} table is different from the JSON file")


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
    
    return transactions

# Display transactions in a DataFrame
def display_transactions(transactions):
    if transactions:
        df = pd.DataFrame(transactions)
        print(df)
        return df
    else:
        print("No transactions found for the specified zip code and date range.")
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

# Modify the existing account details of a customer
def modify_account_details(connection, customer_ssn):
    cursor = connection.cursor(dictionary=True)
    cursor.execute("SELECT * FROM CDW_SAPP_CUSTOMER WHERE SSN = %s", (customer_ssn,))
    account_details = cursor.fetchone()
    cursor.close()

    if not account_details:
        print("Customer not found.")
        return

    fields = ['FIRST_NAME', 'MIDDLE_NAME', 'LAST_NAME', 'FULL_STREET_ADDRESS', 'CUST_CITY', 'CUST_STATE', 'CUST_COUNTRY', 'CUST_ZIP', 'CUST_PHONE', 'CUST_EMAIL']
    new_details = {}

    for field in fields:
        update = input(f"Do you want to update {field} (current value: {account_details[field]})? (y/n): ")
        if update.lower() == 'y':
            new_value = input(f"Enter new value for {field}: ")
            new_details[field] = new_value
        else:
            new_details[field] = account_details[field]

    cursor = connection.cursor()
    update_query = """
    UPDATE CDW_SAPP_CUSTOMER
    SET FIRST_NAME = %s, MIDDLE_NAME = %s, LAST_NAME = %s, 
        FULL_STREET_ADDRESS = %s, CUST_CITY = %s, 
        CUST_STATE = %s, CUST_COUNTRY = %s, CUST_ZIP = %s, 
        CUST_PHONE = %s, CUST_EMAIL = %s
    WHERE SSN = %s
    """
    cursor.execute(update_query, (
        new_details['FIRST_NAME'], new_details['MIDDLE_NAME'], new_details['LAST_NAME'],
        new_details['FULL_STREET_ADDRESS'], new_details['CUST_CITY'],
        new_details['CUST_STATE'], new_details['CUST_COUNTRY'], new_details['CUST_ZIP'],
        new_details['CUST_PHONE'], new_details['CUST_EMAIL'], customer_ssn
    ))
    connection.commit()
    cursor.close()

    print("Account details updated.")

# Generate a monthly bill for a credit card number for a given month and year
def generate_monthly_bill(connection, card_number, month_year):
    month, year = month_year.split('-')
    month_year_prefix = f"{year}{month.zfill(2)}"
    
    cursor = connection.cursor()
    
    query = """
    SELECT SUM(TRANSACTION_VALUE) AS total_amount 
    FROM CDW_SAPP_CREDIT_CARD
    WHERE CREDIT_CARD_NO = %s AND TIMEID LIKE %s
    """
    
    cursor.execute(query, (card_number, f"{month_year_prefix}%"))
    total_amount = cursor.fetchone()[0]
    cursor.close()
    
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