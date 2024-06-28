from functions import *

# Database connection to create database
connection = mysql.connector.connect(
    host="localhost",
    user=my_secrets.mysql_username,
    password=my_secrets.mysql_password
)

create_database(connection, "creditcard_capstone")

# Initialize Spark session
spark = initialize_spark_session('Credit Card System')

# Define the schema for the JSON files
branch_schema, credit_schema, customer_schema = get_schema()

# Define the JDBC URL and connection properties
jdbc_url = "jdbc:mysql://localhost:3306/creditcard_capstone"
connection_properties = {
    "user": my_secrets.mysql_username,
    "password": my_secrets.mysql_password,
    "driver": "com.mysql.cj.jdbc.Driver"
}

# Read JSON files into DataFrames
branch_df = read_json_to_df(spark, branch_schema, "../data/cdw_sapp_branch.json")
credit_card_df = read_json_to_df(spark, credit_schema, "../data/cdw_sapp_credit.json")
customer_df = read_json_to_df(spark, customer_schema, "../data/cdw_sapp_customer.json")

# Transform and load data
transform_and_load_branch_data(branch_df, jdbc_url, connection_properties)
transform_and_load_credit_card_data(credit_card_df, jdbc_url, connection_properties)
transform_and_load_customer_data(customer_df, jdbc_url, connection_properties)

# Stop the Spark session
spark.stop()

# Check if there are differences between the JSON files and the MySQL database
branch_json = load_json("../data/cdw_sapp_branch.json")
credit_card_json = load_json("../data/cdw_sapp_credit.json")
customer_json = load_json("../data/cdw_sapp_customer.json")

# Check if the number of rows in the JSON files and the MySQL database are the same
cursor = connection.cursor()
cursor.execute("USE creditcard_capstone")
check_row_count(cursor, "CDW_SAPP_BRANCH", len(branch_json))
check_row_count(cursor, "CDW_SAPP_CREDIT_CARD", len(credit_card_json))
check_row_count(cursor, "CDW_SAPP_CUSTOMER", len(customer_json))

# Close the cursor and connection
cursor.close()
connection.close()