### Introduction
In this project, the data is getting loaded in json data format from AWS S3(Simple Storage Service) in Spark Cluster and written back to S3 in parquet format. For running optimized queries for song analysis, I have designed star schema with **songplays** as fact table and **users**, **songs**, **artists**, **time** as dimension tables.
![alt text](https://udacity-reviews-uploads.s3.us-west-2.amazonaws.com/_attachments/38715/1584369284/Song_ERD.png)

### Details

Description for each file:

* **etl.py:** Reads and processes all files from song_data and log_data and loads the data into spark. After processing data writes them back to S3.

### Execution

To run pipeline: `python etl.py`






