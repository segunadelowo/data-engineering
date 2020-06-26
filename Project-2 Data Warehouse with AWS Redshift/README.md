# Project summary

A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

They want a data engineer to create a AWS Redshift database with tables designed to optimize queries on song play analysis due to the anticpated growth of user
acitivites.

As the selected data engineer I created a database schema and ETL pipeline for this analysis. I tested the database and ETL pipeline by running queries given to me by the analytics team from Sparkify and compared my results with their expected results.


# Python script execution

To create the needed AWS Redshift tables and data via ETL pipeline:

    - Step 1: Create tables by running "python3 create_tables.py"
    - Step 2: Populate the tables using ETL by running "python3 etl.py"

# Repository files
The list below are the files in the repository
* **create_tables.py**: This is a Python script that contains SQL statements and schema definition used in creating or re-creating the database with tables.
* **sql_queries.py**: This is a Python script that contains SQL statements that is used by the scripts create_tables.py and etl.py
* **etl.py**: This is a Python script that connects to the S3 data scource to extract data from the Song and Log folders which both have json files. This script also transforms and inserts the extracted records into the created tables in the AWS Redshift database.

# The need for the Sparkify database

The Sparkify analytics team is particularly interested in understanding what songs users are listening to in the growing database. Currently, they don't have an easy way to query their data. Creating a database which also is populated by the ETL system along with a Star schema that enables joins and aggregations which makes it easy to carry out data analysis.

The Star schema design:

    - Dimension tables are: users, songs, artists and time
    - Fact table is: songplays