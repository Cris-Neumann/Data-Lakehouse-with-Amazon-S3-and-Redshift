import pandas as pd
import configparser
import psycopg2
import sys
import os

path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
os.chdir(path)
sys.path.append(path)
from sql_queries.queries_etl import copy_table_queries, insert_dim_queries, insert_fact_queries, test_queries

def copy_insert_tables(conn:psycopg2.extensions.connection, cur:psycopg2.extensions.cursor, queries_list:list):
    """ Perform operations on tables (insert or copy) from a list of queries.
    Args:
        conn (psycopg2.extensions.connection): Connection to Postgres DB.
        cur (psycopg2.extensions.cursor): Cursor to send commands to Postgres DB.
        queries_list (list): List of queries to execute.
    Returns:
        None
    """
    for query in queries_list:
        cur.execute(query)
        conn.commit()
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
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    copy_insert_tables(cur, conn, copy_table_queries)
    copy_insert_tables(cur, conn, insert_dim_queries)
    copy_insert_tables(cur, conn, insert_fact_queries)
    test_tables(conn, test_queries)
    conn.close()

if __name__ == "__main__":
    main()
