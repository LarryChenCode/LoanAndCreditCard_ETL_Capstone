# Loan and Credit Card ETL Capstone Project

## Overview
This capstone project demonstrates the knowledge and abilities acquired throughout the Data Engineering course. The project involves managing an ETL process for a Loan Application dataset and a Credit Card dataset using Python, SQL, Apache Spark, and various Python libraries for data visualization and analysis.

## Authors
- Chun-hao (Larry) Chen &nbsp;<a href="https://www.linkedin.com/in/larrychencpa/"><img src="https://upload.wikimedia.org/wikipedia/commons/c/ca/LinkedIn_logo_initials.png" alt="LinkedIn" style="height: 1em; width:auto;"/></a> &nbsp; <a href="https://github.com/LarryChenCode"> <img src="https://upload.wikimedia.org/wikipedia/commons/9/91/Octicons-mark-github.svg" alt="GitHub" style="height: 1em; width: auto;"/></a>

## Table of Contents
- [Business Use Case](#business-use-case)
- [Technologies Used](#technologies-used)
- [Project Workflow](#project-workflow)
- [Project Structure](#project-structure)
- [Data Extraction and Transformation](#data-extraction-and-transformation)
- [Data Loading into Database](#data-loading-into-database)
- [Application Front-End](#application-front-end)
- [Data Analysis and Visualization](#data-analysis-and-visualization)
- [Reference](#reference)

## Business Use Case
Any financial institution that wants to improve operational efficiency and gain real-time customer insights can benefit from an optimized data processing pipeline like the one we built. This solution is particularly valuable for banks and credit card companies but can also be adapted for other industries dealing with large volumes of transactional data. Here are a couple of specific business use cases:

- Chase Bank (Banking) - Enhance fraud detection by implementing a real-time data processing pipeline with PySpark and Kafka.
- American Express (Credit Card Company) - Optimize customer targeting for marketing campaigns using an ETL pipeline and advanced analytics for customer segmentation.

## Technologies Used
- Python (Pandas, Matplotlib, Seaborn, FPDF)
- SQL (MySQL)
- PySpark
- API

## Project Workflow
### This project involves the following steps:
1. Data extraction, transformation, and loading using Python (Pandas, advanced modules like Matplotlib, Seaborn, FPDF), SQL, and PySpark.
2. Creating a console-based application for data management and visualization.
3. Developing visualizations and analytics using Python libraries.

### Workflow Diagram
<!-- <img width="438" alt="image" src="https://github.com/user-attachments/assets/6ea2e4ad-c5ac-4e59-9fc0-02babd546b8e"> -->

```mermaid
graph TB
  subgraph "Data Sources"
    A[CDW_SAPP_CUSTOMER Json file] -->|E: Extraction| B
    C[CDW_SAPP_CREDITCARD Json file] -->|E: Extraction| B
    D[CDW_SAPP_BRANCH Json file] -->|E: Extraction| B
    E[CDW_SAPP_LOAN Data API Endpoint] -->|Python Rest API| B
  end
  
  subgraph "Processing"
    B[Apache Spark
    T: Transformation]
  end
  
  B -->|L: Load| F[MySQL Database]
  F -->|C| G[CLI Python Program]
  F -->|D| H[Data analyzing and Visualization]
```

## Project Structure
- `data/`: Contains the JSON files for Credit Card dataset.
- `db/`: Contains the sql file for database creditcard_capstone 
- `src/`: Contains the Python scripts for data ETL process, application front-end, and data analysis and visualization.
- `report/`: Directory for saving generated reports.
- `image/`: Directory for saving visualizations.

## Data Extraction and Transformation
The project reads data from the JSON files and API:

### Data: 
1. JSON files:
    - `CDW_SAPP_BRANCH.JSON`
    - `CDW_SAPP_CREDITCARD.JSON`
    - `CDW_SAPP_CUSTOMER.JSON`
2. API Endpoint:\
https://raw.githubusercontent.com/platformps/LoanDataset/main/loan_data.json

The data extraction and transformation processes were conducted using PySpark, following the specifications outlined in the [mapping document](https://docs.google.com/spreadsheets/d/1t8UxBrUV6dxx0pM1VIIGZpSf4IKbzjdJ/edit?usp=sharing&ouid=112747427162279359902&rtpof=true&sd=true). Schemas were defined for each file, and data was read into Spark DataFrames.

### Schemas code:
- Data from Json:
![image](https://github.com/user-attachments/assets/fe1c17e1-5071-4abd-9990-f03c8817452c)
- Data from API:
![image](https://github.com/user-attachments/assets/9940a7c4-6758-457c-9a7e-94cf5c76034c)


## Data Loading into Database
Transformed data is loaded into a MySQL database named creditcard_capstone with the following tables:
- `CDW_SAPP_BRANCH`
- `CDW_SAPP_CREDIT_CARD`
- `CDW_SAPP_CUSTOMER`
- `CDW_SAPP_loan_application`

Database and tables in MySQL:

https://github.com/user-attachments/assets/a572fa62-11ad-408e-907b-d1488982cfbe

## Application Front-End
A console-based menu-driven application is created to interact with the data. The application includes the following modules:

### Transaction Details Module:
#### Functions:
- Query transactions based on zip code and month-year.
- Display and export transactions.

#### Demo video
https://github.com/user-attachments/assets/452a4f85-2337-4e48-9a01-b9ad00cba9b1

### Customer Details Module:
#### Functions:
- Check and modify existing account details.
- Generate monthly bills and export to PDF and CSV.
- Display transactions between two dates and export to CSV.

#### Demo video
https://github.com/user-attachments/assets/c7b90c11-1d54-49c8-84b9-3cd9486b3ae8

## Data Analysis and Visualization
The data analysis revealed key transaction patterns, with bills, healthcare, and test transactions being the most frequent. New York had the highest number of customers, while Daly Leticia Fly led in transaction sums. Loan approval rates for self-employed individuals were 65.71%, with 28.43% of married male applicants facing rejection. February, May, and October 2018 saw the highest transaction volumes, and branch code 25 handled the most healthcare transactions.

### Graphs: 
### Transaction Type Analysis
The top transaction types are bills (6861), healthcare (6723), and tests (6683).
![Transaction Type Count](image/3_1_transaction_type_count.png)

### Top 10 States by Customer Count
New York leads with 96 customers.
![Top 10 States by Customers](image/3_2_top_10_states_customers.png)

### Top 10 Customers by Transaction Sum
Daly Leticia Fly has the highest transaction sum of $5,633.07.
![Top 10 Customers by Transaction Sum](image/3_3_top_10_customers_transaction_sum.png)

### Self-Employed Approval Percentage
65.71% of loan applications from self-employed applicants are approved.
![Self-Employed Approval Percentage](image/5_1_self_employed_approval_percentage_pie.png)

### Married Male Rejection Percentage
28.43% of loan applications from married male applicants are rejected.
![Married Male Rejection Percentage](image/5_2_married_male_rejection_percentage_pie.png)

### Top Three Months by Transaction Volume
February 2018 (3959), May 2018 (3946), and October 2018 (3946) have the highest transaction volumes.
![Top Three Months by Transaction Volume](image/5_3_top_three_months_transaction_volume.png)

### The Branch with the Highest Healthcare Transactions Amount
Branch code 25 leads with a total value of $4,370.18 in healthcare transactions.
![Highest Healthcare Transactions Branch](image/5_4_highest_healthcare_transactions_branch.png)

## Reference
PySpark:\
https://spark.apache.org/docs/latest/api/python/index.html

Apache Spark - Spark SQL:\
https://spark.apache.org/sql/

Analyzing and Visualization:\
https://www.analyticsvidhya.com/blog/2021/08/understanding-bar-plots-in-python-beginnersguide-to-data-visualization/

How to make a Todo List CLI application using Python ?\
https://www.geeksforgeeks.org/how-to-make-a-todo-list-cli-application-using-python/

Per Scholas Data Engineering Program:\
https://perscholas.org/
