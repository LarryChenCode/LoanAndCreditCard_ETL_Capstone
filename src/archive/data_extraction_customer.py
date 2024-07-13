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

def insert_customer_data(customer_data):
    for customer in customer_data:
        ssn = customer["SSN"]
        first_name = customer["FIRST_NAME"].title() 
        middle_name = customer["MIDDLE_NAME"].lower()
        last_name = customer["LAST_NAME"].title()
        credit_card_no = customer["CREDIT_CARD_NO"]
        full_street_address = f"{customer['APT_NO']} {customer['STREET_NAME']}"  # Concatenate apartment number and street name
        cust_city = customer["CUST_CITY"]
        cust_state = customer["CUST_STATE"]
        cust_country = customer["CUST_COUNTRY"]
        cust_zip = customer["CUST_ZIP"]
        cust_phone = f"({str(customer['CUST_PHONE'])[:3]}){str(customer['CUST_PHONE'])[3:6]}-{str(customer['CUST_PHONE'])[6:]}"  # Format phone number
        cust_email = customer["CUST_EMAIL"]
        last_updated = customer["LAST_UPDATED"]

        insert_query = """
        INSERT INTO CDW_SAPP_CUSTOMER (SSN, FIRST_NAME, MIDDLE_NAME, LAST_NAME, CREDIT_CARD_NO, FULL_STREET_ADDRESS, CUST_CITY, CUST_STATE, CUST_COUNTRY, CUST_ZIP, CUST_PHONE, CUST_EMAIL, LAST_UPDATED)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        cursor.execute(insert_query, (ssn, first_name, middle_name, last_name, credit_card_no, full_street_address, cust_city, cust_state, cust_country, cust_zip, cust_phone, cust_email, last_updated))
        connection.commit()
        print(f"Inserted customer with SSN: {ssn}")

def load_json(file_path):
    with open(file_path, "r") as file:
        return json.load(file)

# Load the json file
customer = load_json("../data/cdw_sapp_customer.json")

# calculate the total number of branches from the json file
total_customers = len(customer)
print(f"Total number of customers (JSON file): {total_customers}")

# Insert the branch data into the database
insert_customer_data(customer)

# Caluculate the total number of branches
cursor = connection.cursor()
cursor.execute("SELECT COUNT(*) FROM CDW_SAPP_CUSTOMER")
result = cursor.fetchone()
print(f"Total number of customers (MySQL database): {result[0]}")

cursor.close()
connection.close()