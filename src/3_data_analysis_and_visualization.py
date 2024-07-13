from functions import *

connection = define_connection_with_db()

plot_transaction_type_count(connection)

plot_top_states_with_customers(connection)

plot_top_customers_by_transaction_sum(connection)

connection.close()