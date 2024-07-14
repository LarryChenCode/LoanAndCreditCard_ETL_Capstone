# Loan and Credit Card ETL Capstone Project

## Overview
This capstone project demonstrates the knowledge and abilities acquired throughout the Data Engineering course. The project involves managing an ETL process for a Loan Application dataset and a Credit Card dataset using Python, SQL, Apache Spark, and various Python libraries for data visualization and analysis.

## Authors
- Chun-hao (Larry) Chen &nbsp;<a href="https://www.linkedin.com/in/larrychencpa/"><img src="https://upload.wikimedia.org/wikipedia/commons/c/ca/LinkedIn_logo_initials.png" alt="LinkedIn" style="height: 1em; width:auto;"/></a> &nbsp; <a href="https://github.com/LarryChenCode"> <img src="https://upload.wikimedia.org/wikipedia/commons/9/91/Octicons-mark-github.svg" alt="GitHub" style="height: 1em; width: auto;"/></a>

## Table of Contents
- [Technologies Used](#technologies-used)
- [Project Structure](#project-structure)
- [Data Extraction and Transformation](#data-extraction-and-transformation)
- [Loan Application Dataset - API](#loan-application-dataset)
- [Data Loading into Database](#data-loading-into-database)
- [Application Front-End](#application-front-end)
- [Data Analysis and Visualization](#data-analysis-and-visualization)
- [Demo Video](#demo-video)


## Summary
This project involves the following steps:
1. Preprocessing of datasets (Loan Application and Credit Card).
2. Data extraction, transformation, and loading using Python (Pandas, advanced modules like Matplotlib), SQL, and Apache Spark (Spark Core, Spark SQL).
3. Creating a console-based application for data management and visualization.
4. Developing visualizations and analytics using Python libraries.

## Technologies Used
- Python (Pandas, Matplotlib, Seaborn, FPDF)
- SQL (MySQL)
- PySpark
- API

## Project Structure
- `data/`: Contains the JSON files for Credit Card dataset.
- `db/`: Contains the sql file for database creditcard_capstone 
- `src/`: Contains the Python scripts for data extraction, transformation, and loading.
- `report/`: Directory for saving generated reports.
- `image/`: Directory for saving visualizations.

## Data Extraction and Transformation
The project reads data from the following JSON files:
- `CDW_SAPP_BRANCH.JSON`
- `CDW_SAPP_CREDITCARD.JSON`
- `CDW_SAPP_CUSTOMER.JSON`
Data extraction and transformation are performed using PySpark. Schemas are defined for each file, and data is read into Spark DataFrames.

## Loan Application Dataset - API
Data is fetched from a Loan Application API and loaded into the CDW_SAPP_loan_application table in the creditcard_capstone database. The data is then analyzed and visualized. 

## Data Loading into Database
Transformed data is loaded into a MySQL database named creditcard_capstone with the following tables:
- `CDW_SAPP_BRANCH`
- `CDW_SAPP_CREDIT_CARD`
- `CDW_SAPP_CUSTOMER`

## Application Front-End
A console-based menu-driven application is created to interact with the data. The application includes the following modules:

### Transaction Details Module:
- Query transactions based on zip code and month-year.
- Display and export transactions.

### Customer Details Module:
- Check and modify existing account details.
- Generate monthly bills and export to PDF and CSV.
- Display transactions between two dates and export to CSV.

## Data Analysis and Visualization
The project includes several visualizations to analyze the data. Below are the visualizations created:

### Transaction Type Analysis
![](image\3_1_transaction_type_count.png)
Plot showing which transaction type has the highest transaction count.

### Top 10 States by Customer Count
![](image\3_2_top_10_states_customers.png)
Plot showing the top 10 states with the highest number of customers.

### Top 10 Customers by Transaction Sum
![](image\3_3_top_10_customers_transaction_sum.png)
Plot showing the top 10 customers with the highest transaction sums.

### Self-Employed Approval Percentage
![](image\5_1_self_employed_approval_percentage_pie.png)
Plot showing the percentage of applications approved for self-employed applicants.

### Married Male Rejection Percentage
![](image\5_2_married_male_rejection_percentage_pie.png)
Plot showing the rejection percentage for married male applicants.

### Top Three Months by Transaction Volume
![](image\5_3_top_three_months_transaction_volume.png)
Plot showing the top three months with the largest volume of transactions.

#### The Highest Healthcare Transactions by Branch
![](image\5_4_highest_healthcare_transactions_branch.png)
Plot showing which branch processed the highest total value of healthcare transactions.

## Demo Video
