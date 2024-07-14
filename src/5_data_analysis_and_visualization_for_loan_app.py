from functions import *

connection = define_connection_with_db()

plot_self_employed_approval_percentage(connection)
plot_married_male_rejection_percentage(connection)
plot_top_three_months_transaction_volume(connection)
plot_highest_healthcare_transactions_branch(connection)

connection.close()