import mysql.connector
import my_secrets

# Establish a connection to the MySQL server
connection = mysql.connector.connect(
    host="localhost",
    user=my_secrets.mysql_username,
    password=my_secrets.mysql_password
)

# Check if the connection is successful
if connection.is_connected():
    print("Successfully Connected to MySQL server")
else:
    print("Failed to connect to MySQL server")

# Create a new database
cursor = connection.cursor()
cursor.execute("CREATE DATABASE creditcard_capstone")
print("Database 'creditcard_capstone' created")

# Switch to the new database
cursor.execute("USE creditcard_capstone")

# Create the CDW_SAPP_BRANCH table
create_table_query = """
CREATE TABLE CDW_SAPP_BRANCH (
    BRANCH_CODE INT PRIMARY KEY,
    BRANCH_NAME VARCHAR(255),
    BRANCH_STREET VARCHAR(255),
    BRANCH_CITY VARCHAR(255),
    BRANCH_STATE VARCHAR(255),
    BRANCH_ZIP VARCHAR(255) DEFAULT '99999',
    BRANCH_PHONE VARCHAR(255),
    LAST_UPDATED TIMESTAMP
)
"""
cursor.execute(create_table_query)
print("CDW_SAPP_BRANCH table created")

# Create the CDW_SAPP_CREDIT_CARD table
create_table_query = """
CREATE TABLE CDW_SAPP_CREDIT_CARD (
    CUST_CC_NO VARCHAR(255),
    TIMEID VARCHAR(8),
    CUST_SSN INT,
    BRANCH_CODE INT,
    TRANSACTION_TYPE VARCHAR(255),
    TRANSACTION_VALUE DOUBLE,
    TRANSACTION_ID INT PRIMARY KEY
)
"""
cursor.execute(create_table_query)
print("CDW_SAPP_CREDIT_CARD table created")

# Create the CDW_SAPP_CUSTOMER table
create_table_query = """
CREATE TABLE CDW_SAPP_CUSTOMER (
    SSN INT PRIMARY KEY,
    FIRST_NAME VARCHAR(255),
    MIDDLE_NAME VARCHAR(255),
    LAST_NAME VARCHAR(255),
    CREDIT_CARD_NO VARCHAR(255),
    FULL_STREET_ADDRESS VARCHAR(255),
    CUST_CITY VARCHAR(255),
    CUST_STATE VARCHAR(255),
    CUST_COUNTRY VARCHAR(255),
    CUST_ZIP VARCHAR(255),
    CUST_PHONE VARCHAR(255),
    CUST_EMAIL VARCHAR(255),
    LAST_UPDATED TIMESTAMP
)
"""
cursor.execute(create_table_query)
print("CDW_SAPP_CUSTOMER table created")

cursor.close()
connection.close()