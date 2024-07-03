from functions import *

connection = mysql.connector.connect(
    host="localhost",
    user=my_secrets.mysql_username,
    password=my_secrets.mysql_password,
    database="creditcard_capstone"
)

plot_transaction_type_count(connection)

plot_top_states_with_customers(connection)

plot_top_customers_by_transaction_sum(connection)

connection.close()