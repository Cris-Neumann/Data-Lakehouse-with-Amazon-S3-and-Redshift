import pandas as pd
import configparser
import psycopg2
import sys
import os

path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
os.chdir(path)
sys.path.append(path)
from sql_queries.queries_etl import copy_table_queries, insert_dim_queries, insert_fact_queries, test_queries

def copy_insert_tables(cur:psycopg2.extensions.cursor, queries_list:list):
    """ Perform operations on tables (insert or copy) from a list of queries.
    Args:
        cur (psycopg2.extensions.cursor): Cursor to send commands to Postgres DB.
        queries_list (list): List of queries to execute.
    Returns:
        None
    """
    for query in queries_list:
        cur.execute(query)
    return None

def test_tables(conn:psycopg2.extensions.connection, test_queries:list):
    """ Test Data Warehouse tables for data records.
    Args:
        conn (psycopg2.extensions.connection): Connection to Postgres DB.
        test_queries (list): List of queries to execute.
    Returns:
        None
    """
    for query in test_queries:
        data = pd.read_sql(query, conn)
        print(data)
    return None

def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['REDSHIFT_CLUSTER'].values()))
    cur = conn.cursor()
    copy_insert_tables(cur, copy_table_queries)
    copy_insert_tables(cur, insert_dim_queries)
    copy_insert_tables(cur, insert_fact_queries)
    test_tables(conn, test_queries)
    conn.commit()
    conn.close()

if __name__ == "__main__":
    main()
