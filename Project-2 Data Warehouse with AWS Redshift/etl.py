import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    This procedure executes the sql query to read data from the S3 datasource.

    INPUTS: 
    * cur the cursor variable
    * conn db connection 
    """
    
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    This procedure executes the sql query to insert copied data into the 
    Redshift staging tables.

    INPUTS: 
    * cur the cursor variable
    * conn db connection 
    """
    
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    This procedure is the entry point of the program
    all the other procedures are executed via this procedure
    """
    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    DWH_DB=config['DWH']['DWH_DB']
    DWH_DB_USER=config['DWH']['DWH_DB_USER']
    DWH_DB_PASSWORD=config['DWH']['DWH_DB_PASSWORD']
    DWH_PORT=config['DWH']['DWH_PORT']
    HOST=DWH_DB=config['DWH']['HOST']

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(HOST,DWH_DB,DWH_DB_USER,DWH_DB_PASSWORD,DWH_PORT))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()