# Project summary

A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, the company has grown its user base and song database even more and wants to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

They want a data engineer to building an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.

As the selected data engineer I created a storage schema and ETL pipeline for this analysis. I tested the S3 storage and Spark ETL pipeline by running queries given to me by the analytics team from Sparkify and compared my results with their expected results.


# Python script execution

To execute the needed Spark functions and data processing via ETL pipeline:

    - Step 1: Process the records in the S3 bucket using ETL by running "python etl.py"

# Repository files
* **etl.py**: This is a Python script that connects to the S3 data scource to extract data from the Song and Log folders which both have json files. This script also transforms the data using Spark and writes the processed records into the S3 bucket.

# The need for the Sparkify database

The Sparkify analytics team is particularly interested in properly managing the company data that has grown and want to move their data warehouse to a data lake.. Currently, they don't have an easy way to query their data. Creating a datalake which also is populated by the ETL system along with a Star schema that enables joins and aggregations which makes it easy to carry out data analysis.

The Star schema design:

    - Dimension tables are: users, songs, artists and time
    - Fact table is: songplays