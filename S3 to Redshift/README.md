### Introduction
In this project I have loaded json data from AWS S3(Simple Storage Service) in AWS Redshift staging tables and inserted data in analytics tables in Redshift. For running optimized queries for song analysis, I have designed star schema with **songplays** as fact table and **users**, **songs**, **artists**, **time** as dimension tables.
![alt text](https://udacity-reviews-uploads.s3.us-west-2.amazonaws.com/_attachments/38715/1584369284/Song_ERD.png)

### Details

Description for each file:

* **create_tables.py:** Drops all the table and creates new table, providing a way to quickly reset the table while building etl pipeline.
* **etl.py:** Reads and processes all files from song_data and log_data and loads the data into respective tables. This python script created based on etl.ipynb notebook.
* **sql_queries:** Contains queries to perform following tasks: 
    * create table 
    * drop table 
    * copying data from S3 in redshift staging tables 
    * Performing sql-sql ETL to load data from staging tables to analytics tables in redshift

### Execution

To create table: `python create_tables.py`

To run pipeline and load data in database: `python etl.py`






