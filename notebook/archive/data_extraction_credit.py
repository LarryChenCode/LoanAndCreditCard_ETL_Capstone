# load the json file
import json
import mysql.connector
import my_secrets

# Establish a connection to the MySQL server
connection = mysql.connector.connect(
    host="localhost",
    user=my_secrets.mysql_username,
    password=my_secrets.mysql_password,
    database="creditcard_capstone"
)

# Check if the connection is successful
if connection.is_connected():
    print("Successfully Connected to MySQL server")
else:
    print("Failed to connect to MySQL server")

cursor = connection.cursor()

def insert_credit_card_data(credit_card_data):
    for transaction in credit_card_data:
        cust_cc_no = transaction["CREDIT_CARD_NO"]
        timeid = f"{transaction['YEAR']:04d}{transaction['MONTH']:02d}{transaction['DAY']:02d}"
        cust_ssn = transaction["CUST_SSN"]
        branch_code = transaction["BRANCH_CODE"]
        transaction_type = transaction["TRANSACTION_TYPE"]
        transaction_value = transaction["TRANSACTION_VALUE"]
        transaction_id = transaction["TRANSACTION_ID"]

        insert_query = """
        INSERT INTO CDW_SAPP_CREDIT_CARD (CUST_CC_NO, TIMEID, CUST_SSN, BRANCH_CODE, TRANSACTION_TYPE, TRANSACTION_VALUE, TRANSACTION_ID)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        cursor.execute(insert_query, (cust_cc_no, timeid, cust_ssn, branch_code, transaction_type, transaction_value, transaction_id))
        connection.commit()
        print(f"Inserted transaction with TRANSACTION_ID: {transaction_id}")

def load_json(file_path):
    with open(file_path, "r") as file:
        return json.load(file)

# Load the json file
transaction = load_json("../data/cdw_sapp_credit.json")

# calculate the total number of branches from the json file
total_transactions = len(transaction)
print(f"Total number of transactions (JSON file): {total_transactions}")

# Insert the branch data into the database
insert_credit_card_data(transaction)

# Caluculate the total number of branches
cursor = connection.cursor()
cursor.execute("SELECT COUNT(*) FROM CDW_SAPP_CREDIT_CARD")
result = cursor.fetchone()
print(f"Total number of transactions (MySQL database): {result[0]}")

cursor.close()
connection.close()