import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    This procedure drops the tables created for etl.

    INPUTS: 
    * cur the cursor variable
    * conn db connection 
    """
    
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    This procedure creates the tables needed for etl.

    INPUTS: 
    * cur the cursor variable
    * conn db connection 
    """
    
    for query in create_table_queries:
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

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()