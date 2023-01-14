import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Executes SQL statements to drop all tables in the database if they exist.
  
    Parameters:
    cur (Cursor): SQL-connected cursor.
    con (Connection): Connection to Redshift Cluster.
  
    Returns:
    None (Null)
  
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Executes SQL statements to create all tables in the database if they do not exist.
  
    Parameters:
    cur (Cursor): SQL-connected cursor.
    con (Connection): Connection to Redshift Cluster.
  
    Returns:
    None (Null)
  
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Establishes a connection and SQL cursor with respect to the Redshift Cluster defined in dwh.cfg. Deletes relevant tables if they exists and creates new tables in their place.
  
    Parameters:
    -
  
    Returns:
    None (Null)
  
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()

if __name__ == "__main__":
    main()