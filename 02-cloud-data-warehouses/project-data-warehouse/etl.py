import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries
from tests.test import test_counts

def load_staging_tables(cur, conn):
    """Loads staging tables

    Args:
          cur (cursor): cursor object
          conn (connection): connection object
    """
    print('Loading staging tables')
    for query in copy_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """Insert tables to the database

    Args:
          cur (cursor): cursor object
          conn (connection): connection object
    """
    print('Inserting tables')
    for query in insert_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()


def main():
    """Main function to run the ETL process
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)
    
    test_counts(cur, conn)

    conn.close()
    print('ETL process completed')


if __name__ == "__main__":
    main()