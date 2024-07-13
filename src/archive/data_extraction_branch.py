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

def insert_branch_data(branch_data):
    for branch in branch_data:
        branch_code = branch["BRANCH_CODE"]
        branch_name = branch["BRANCH_NAME"]
        branch_street = branch["BRANCH_STREET"]
        branch_city = branch["BRANCH_CITY"]
        branch_state = branch["BRANCH_STATE"]
        branch_zip = str(branch["BRANCH_ZIP"]).zfill(5) if branch["BRANCH_ZIP"] else "99999"
        branch_phone = f"({branch['BRANCH_PHONE'][:3]}){branch['BRANCH_PHONE'][3:6]}-{branch['BRANCH_PHONE'][6:]}"  # Format phone number
        last_updated = branch["LAST_UPDATED"]

        insert_query = """
        INSERT INTO CDW_SAPP_BRANCH (BRANCH_CODE, BRANCH_NAME, BRANCH_STREET, BRANCH_CITY, BRANCH_STATE, BRANCH_ZIP, BRANCH_PHONE, LAST_UPDATED)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        cursor.execute(insert_query, (branch_code, branch_name, branch_street, branch_city, branch_state, branch_zip, branch_phone, last_updated))
        connection.commit()
        print(f"Inserted branch with BRANCH_CODE: {branch_code}")

def load_json(file_path):
    with open(file_path, "r") as file:
        return json.load(file)

# Load the json file
branch = load_json("../data/cdw_sapp_branch.json")


# calculate the total number of branches from the json file
total_branches = len(branch)
print(f"Total number of branches (JSON file): {total_branches}")

# Insert the branch data into the database
insert_branch_data(branch)

# Caluculate the total number of branches
cursor = connection.cursor()
cursor.execute("SELECT COUNT(*) FROM CDW_SAPP_BRANCH")
result = cursor.fetchone()
print(f"Total number of branches (MySQL database): {result[0]}")

cursor.close()
connection.close()