from functions import *

api_url = "https://raw.githubusercontent.com/platformps/LoanDataset/main/loan_data.json"

# Fetch the loan data
loan_data, status_code = get_loan_data(api_url)

if loan_data:
    # Initialize Spark session
    spark = initialize_spark_session('Loan Application Data')

    # Define JDBC URL and connection properties
    jdbc_url, connection_properties = define_jdbc_n_properties()

    # Transform and load the loan data into the database
    transform_and_load_loan_data(spark, loan_data, jdbc_url, connection_properties)

    # Stop the Spark session
    spark.stop()