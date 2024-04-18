import configparser
import psycopg2
import sys
import os

path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
os.chdir(path)
sys.path.append(path)
from sql_queries.queries_create_tables import drop_table_queries, create_table_queries

def create_drop_tables(cur:psycopg2.extensions.cursor, queries_list:list):
    """ Perform operations on tables (create or drop) from a list of queries.
    (with SHOW COLUMNS FROM TABLE dev.public.staging_tip see the table in Redshift)
    Args:
        cur (psycopg2.extensions.cursor): Cursor to send commands to Postgres DB.
        queries_list (list): List of queries to execute.
    Returns:
        None
    """
    for query in queries_list:
        cur.execute(query)
    return None

def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['REDSHIFT_CLUSTER'].values()))
    cur = conn.cursor()
    create_drop_tables(cur, drop_table_queries)
    create_drop_tables(cur, create_table_queries)
    conn.commit()
    conn.close()

if __name__ == "__main__":
    main()
