import mysql.connector
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, TimestampType
from pyspark.sql.functions import col, expr, concat_ws, lpad
import json
import my_secrets


def create_database(connection, database_name):
    cursor = connection.cursor()
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database_name}")
    cursor.close()
    print(f"Database '{database_name}' created")


def initialize_spark_session(app_name):
    return SparkSession.builder.appName(app_name).getOrCreate()


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


def read_json_to_df(spark, schema, file_path):
    return spark.read.schema(schema).option("multiline", "true").json(file_path)


def transform_and_load_branch_data(branch_df, jdbc_url, connection_properties):
    branch_transformed_df = branch_df.withColumn("BRANCH_ZIP", lpad(col("BRANCH_ZIP").cast("string"), 5, "0")) \
        .withColumn("BRANCH_PHONE", expr("concat('(', substring(BRANCH_PHONE, 1, 3), ')', substring(BRANCH_PHONE, 4, 3), '-', substring(BRANCH_PHONE, 7, 4))"))
    branch_transformed_df.write.jdbc(url=jdbc_url, table="CDW_SAPP_BRANCH", mode="overwrite", properties=connection_properties)


def transform_and_load_credit_card_data(credit_card_df, jdbc_url, connection_properties):
    credit_card_transformed_df = credit_card_df.withColumn("TIMEID", concat_ws("", col("YEAR"), lpad(col("MONTH").cast("string"), 2, "0"), lpad(col("DAY").cast("string"), 2, "0")))
    credit_card_transformed_df = credit_card_transformed_df.drop("DAY", "MONTH", "YEAR")
    credit_card_transformed_df.write.jdbc(url=jdbc_url, table="CDW_SAPP_CREDIT_CARD", mode="overwrite", properties=connection_properties)


def transform_and_load_customer_data(customer_df, jdbc_url, connection_properties):
    customer_transformed_df = customer_df.withColumn("FIRST_NAME", expr("initcap(FIRST_NAME)")) \
        .withColumn("MIDDLE_NAME", expr("lower(MIDDLE_NAME)")) \
        .withColumn("LAST_NAME", expr("initcap(LAST_NAME)")) \
        .withColumn("FULL_STREET_ADDRESS", concat_ws(" ", col("APT_NO"), col("STREET_NAME"))) \
        .withColumn("CUST_PHONE", expr("concat('614', CUST_PHONE)")) \
        .withColumn("CUST_PHONE", expr("concat('(', substring(CUST_PHONE, 1, 3), ')', substring(CUST_PHONE, 4, 3), '-', substring(CUST_PHONE, 7, 4))"))
    customer_transformed_df = customer_transformed_df.drop("APT_NO", "STREET_NAME")
    customer_transformed_df.write.jdbc(url=jdbc_url, table="CDW_SAPP_CUSTOMER", mode="overwrite", properties=connection_properties)


def load_json(file_path):
    with open(file_path, "r") as file:
        return json.load(file)


def check_row_count(cursor, table_name, expected_count):
    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    result = cursor.fetchone()
    if expected_count == result[0]:
        print(f"Number of rows in {table_name} table is the same as the JSON file")
    else:
        print(f"Number of rows in {table_name} table is different from the JSON file")