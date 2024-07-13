from functions import *

connection = mysql.connector.connect(
    host="localhost",
    user=my_secrets.mysql_username,
    password=my_secrets.mysql_password,
    database="creditcard_capstone"
)

# Generate visualizations
plot_self_employed_approval_percentage(connection)
plot_married_male_rejection_percentage(connection)
plot_top_three_months_transaction_volume(connection)
plot_highest_healthcare_transactions_branch(connection)

connection.close()