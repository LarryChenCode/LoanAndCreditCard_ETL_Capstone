from functions import *

api_url = "https://raw.githubusercontent.com/platformps/LoanDataset/main/loan_data.json"
database_name = "creditcard_capstone"
jdbc_url = f"jdbc:mysql://localhost:3306/{database_name}"
connection_properties = {
    "user": my_secrets.mysql_username,
    "password": my_secrets.mysql_password,
    "driver": "com.mysql.cj.jdbc.Driver"
}

# Initialize Spark session
spark = initialize_spark_session("LoanDataETL")

# Fetch loan data from API
loan_data = get_loan_data(api_url)
if loan_data:
    # Display loan data
    display_loan_data(loan_data)
    
    # Load loan data into RDBMS
    load_loan_data_to_rdbms(spark, loan_data, jdbc_url, connection_properties)

spark.stop()