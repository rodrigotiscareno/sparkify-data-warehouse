import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
   Executes SQL statements to synchronize data from the S3 directory to the Redshift staging tables.
  
    Parameters:
    cur (Cursor): SQL-connected cursor.
    con (Connection): Connection to Redshift Cluster.
  
    Returns:
    None (Null)
  
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
   Executes SQL statements to insert data from the staging tables to the finalized database tables.
  
    Parameters:
    cur (Cursor): SQL-connected cursor.
    con (Connection): Connection to Redshift Cluster.
  
    Returns:
    None (Null)
  
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Establishes a connection and SQL cursor with respect to the Redshift Cluster defined in dwh.cfg. Loads data from an S3 directory to Redshift staging tables to then transfer the data into the finalized database tables.
    
    Parameters:
    -
  
    Returns:
    None (Null)
  
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()