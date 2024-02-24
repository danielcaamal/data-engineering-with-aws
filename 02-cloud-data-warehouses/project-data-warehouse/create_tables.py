import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """Drops all tables in the database

    Args:
        cur (cursor): cursor object
        conn (connection): connection object
    """
    print("Dropping tables")
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """Creates all tables in the database
    
    Args:
        cur (cursor): cursor object
        conn (connection): connection object
    """
    print("Creating tables")
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """Main function to drop and create tables in the database
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()
    print("Tables dropped and created successfully")


if __name__ == "__main__":
    main()