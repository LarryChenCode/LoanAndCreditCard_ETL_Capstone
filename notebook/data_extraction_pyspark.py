from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, lit, lpad, expr, date_format, initcap, lower
from pyspark.sql.types import StructType,StructField, StringType, IntegerType,BooleanType,DoubleType, TimestampType
import mysql.connector
import my_secrets
import json

# Database connection to create database
connection = mysql.connector.connect(
    host="localhost",
    user=my_secrets.mysql_username,
    password=my_secrets.mysql_password
)

# Create database
cursor = connection.cursor()
cursor.execute("CREATE DATABASE IF NOT EXISTS creditcard_capstone")
print("Database 'creditcard_capstone' created")

# Initialize Spark session
spark = SparkSession.builder.appName('Credit Card System').getOrCreate()

# Define the schema for the JSON files
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

# Define the JDBC URL and connection properties
jdbc_url = "jdbc:mysql://localhost:3306/creditcard_capstone"
connection_properties = {
    "user": my_secrets.mysql_username,
    "password": my_secrets.mysql_password,
    "driver": "com.mysql.cj.jdbc.Driver"
}

# Read JSON files into DataFrames
branch_df = spark.read.schema(branch_schema).option("multiline", "true").json("../data/cdw_sapp_branch.json")
credit_card_df = spark.read.schema(credit_schema).option("multiline", "true").json("../data/cdw_sapp_credit.json")
customer_df = spark.read.schema(customer_schema).option("multiline", "true").json("../data/cdw_sapp_customer.json")

# Transform and load branch data
branch_transformed_df = branch_df.withColumn("BRANCH_ZIP", lpad(col("BRANCH_ZIP").cast("string"), 5, "0")) \
    .withColumn("BRANCH_PHONE", expr("concat('(', substring(BRANCH_PHONE, 1, 3), ')', substring(BRANCH_PHONE, 4, 3), '-', substring(BRANCH_PHONE, 7, 4))"))
# Write the transformed data to the database
branch_transformed_df.write.jdbc(url=jdbc_url, table="CDW_SAPP_BRANCH", mode="overwrite", properties=connection_properties)

# Transform and load credit card data
credit_card_transformed_df = credit_card_df.withColumn("TIMEID", concat_ws("", col("YEAR"), lpad(col("MONTH").cast("string"), 2, "0"), lpad(col("DAY").cast("string"), 2, "0")))
# Remove "DAY", "MONTH", and "YEAR" columns
credit_card_transformed_df = credit_card_transformed_df.drop("DAY", "MONTH", "YEAR")
# Write the transformed data to the database
credit_card_transformed_df.write.jdbc(url=jdbc_url, table="CDW_SAPP_CREDIT_CARD", mode="overwrite", properties=connection_properties)

# Transform and load customer data
customer_transformed_df = customer_df.withColumn("FIRST_NAME", expr("initcap(FIRST_NAME)")) \
    .withColumn("MIDDLE_NAME", expr("lower(MIDDLE_NAME)")) \
    .withColumn("LAST_NAME", expr("initcap(LAST_NAME)")) \
    .withColumn("FULL_STREET_ADDRESS", concat_ws(" ", col("APT_NO"), col("STREET_NAME"))) \
    .withColumn("CUST_PHONE", expr("concat('(', substring(CUST_PHONE, 1, 3), ')', substring(CUST_PHONE, 4, 3), '-', substring(CUST_PHONE, 7, 4))"))
# Remove "APT_NO" and "STREET_NAME" columns
customer_transformed_df = customer_transformed_df.drop("APT_NO", "STREET_NAME")
# Write the transformed data to the database
customer_transformed_df.write.jdbc(url=jdbc_url, table="CDW_SAPP_CUSTOMER", mode="overwrite", properties=connection_properties)

# Stop the Spark session
spark.stop()


# check if there are differences between the JSON files and the MySQL database
# Load the JSON files
def load_json(file_path):
    with open(file_path, "r") as file:
        return json.load(file)

branch_json = load_json("../data/cdw_sapp_branch.json")
credit_card_json = load_json("../data/cdw_sapp_credit.json")
customer_json = load_json("../data/cdw_sapp_customer.json")

# Check if the number of rows in the JSON files and the MySQL database are the same
total_branches = len(branch_json)
total_credit_card_transactions = len(credit_card_json)
total_customers = len(customer_json)

# Check the number of rows in the CDW_SAPP_BRANCH table
cursor.execute("USE creditcard_capstone")
cursor.execute("SELECT COUNT(*) FROM CDW_SAPP_BRANCH")
result = cursor.fetchone()
if total_branches == result[0]:
    print("Number of rows in CDW_SAPP_BRANCH table is the same as the JSON file")
else:
    print("Number of rows in CDW_SAPP_BRANCH table is different from the JSON file")

# Check the number of rows in the CDW_SAPP_CREDIT_CARD table
cursor.execute("SELECT COUNT(*) FROM CDW_SAPP_CREDIT_CARD")
result = cursor.fetchone()
if total_credit_card_transactions == result[0]:
    print("Number of rows in CDW_SAPP_CREDIT_CARD table is the same as the JSON file")
else:
    print("Number of rows in CDW_SAPP_CREDIT_CARD table is different from the JSON file")

# Check the number of rows in the CDW_SAPP_CUSTOMER table
cursor.execute("SELECT COUNT(*) FROM CDW_SAPP_CUSTOMER")
result = cursor.fetchone()
if total_customers == result[0]:
    print("Number of rows in CDW_SAPP_CUSTOMER table is the same as the JSON file")
else:
    print("Number of rows in CDW_SAPP_CUSTOMER table is different from the JSON file")

# Close the cursor and connection
cursor.close()
connection.close()