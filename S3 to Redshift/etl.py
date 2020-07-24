import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn, ARN):
    "Loads data from s3 to staging table in redshift"
    for query in copy_table_queries:
        cur.execute(query.format(ARN))
        conn.commit()


def insert_tables(cur, conn):
    """Loads data from staging tables to analytics tables"""
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """Gets db connections 
       - Reads dwh.fg configs files 
       - Loads data from S3 to staging table
       - Loads data from staging table to star schema tables
       """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')       

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()  
    ARN = config.get("IAM_ROLE", "ARN")
    
    load_staging_tables(cur, conn, ARN)
    insert_tables(cur, conn) 

    conn.close()


if __name__ == "__main__":
    main()