import configparser
import psycopg2
import pandas as pd

def test_counts(cur, conn):
    data = {'table': ['staging_events', 'staging_songs', 'songplays', 'users', 'songs', 'artists', 'time'],
            'count': []}
    for table in data['table']:
        cur.execute(f"""
                    select count(*) from {table}
                    """)
        data['count'].append(cur.fetchone()[0])
    
    df = pd.DataFrame(data)
    print(df)
    
def test_aux(cur, conn):
    cur.execute("""
                select * from pg_catalog.stl_load_errors 
                """)
    print(cur.fetchone())

def main():
    """Main function to test the ETL process
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    test_counts(cur, conn)
    # test_aux(cur, conn)
    conn.close()

if __name__ == "__main__":
    main()