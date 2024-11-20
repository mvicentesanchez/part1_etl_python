# part1_etl_python

## Part 1: ETL with Python

## Scenario: 
You have been tasked with building an ETL pipeline to extract, transform, and load data from a CSV file into a SQL database using Python. The CSV file contains information about sales transactions with the following columns: transaction_id, customer_id, product_id, quantity, and timestamp. The destination SQL database has a table named "sales" with columns: transaction_id, customer_id, product_id, quantity, and sale_date.

## Task: 

Write a Python script to perform the ETL process. Your script should accomplish the following tasks:

1.	Create mock data to complete the exercise, ensuring that it reflects realistic sales transaction scenarios.
2.	Read the data from the CSV file.
5.	Transform the data to match the schema of the "sales" table in the SQL database, including handling large datasets efficiently and implementing data quality checks for missing values, outliers, and inconsistencies.
6.	Load the transformed data into the "sales" table in the SQL database, supporting incremental loading to process only new records since the last ETL run and incorporating concurrency for improved performance.
7.	Parameterize the script to allow configuration of database connection details, file paths, and other settings without modifying the code.
8.	Implement error handling and logging mechanisms to handle database constraints, unexpected failures, and ensure comprehensive monitoring of the ETL process.
9.	Incorporate data encryption techniques to secure sensitive information, such as customer IDs or product IDs, during transit and storage.
10.	Handle changes to the schema of the CSV file or the destination database table, ensuring backward and forward compatibility without data loss.
11.	Utilize GitHub for version control, creating a repository to manage the development of the ETL script, including branches for feature development, issue tracking, and collaborative code review.
	
## Requirements:

•	Use Python and any necessary libraries for data manipulation.

•	Optimize the script for performance and scalability.

•	Ensure correctness and completeness of the ETL process.

•	Adhere to best practices for data manipulation, concurrency, error handling, logging, security, and version control using GitHub.

##  Evaluation Criteria:

•	Correctness and completeness of the ETL process.

•	Efficiency and optimization of the Python script, including handling large datasets and implementing concurrency.

•	Handling of errors, database constraints, schema evolution, and version control with GitHub.

•	Adherence to best practices for data manipulation, parameterization, logging, security, and version control.
