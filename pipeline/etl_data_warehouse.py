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
