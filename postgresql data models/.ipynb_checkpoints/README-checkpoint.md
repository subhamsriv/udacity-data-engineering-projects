### Introduction
In this project I have processed songs and log data in json format, modeled data for Postgres and inserted it in Postgres database. For running optimized queries for song analysis, I have designed star schema with **songplays** as fact table and **users**, **songs**, **artists**, **time** as dimension tables.
![alt text](https://udacity-reviews-uploads.s3.us-west-2.amazonaws.com/_attachments/38715/1584369284/Song_ERD.png)

### Details

Description for each file:

* **test.ipynb:** Displays the first few rows of each table.
* **etl.ipynb:** Reads and processes a single file from song_data, log_data and loads the data into respective tables. This ipytho notebook helps us in building etl pipeline interactively.
* **create_tables.py:** Drops all the table and creates new table, providing a way to quickly reset the table while building etl pipeline.
* **etl.py:** Reads and processes all files from song_data and log_data and loads the data into respective tables. This python script created based on etl.ipynb notebook.
* **sql_queries:** Contains create table, insert and select queries. 

### Execution

To create table: `python create_tables.py`

To run pipeline and load data in database: `python etl.py`






