import importlib
import logging
import os
import urllib
from decimal import Decimal

import pandas as pd
import pyodbc

from configs import DB_CONFIG

logger = logging.getLogger(__name__)


def db_conn():
    return pyodbc.connect(f"DSN={DB_CONFIG['dsn']}")

def db_exec(conn, query):
    cus = conn.cursor()
    cus.execute(query)
    conn.commit()

def db_get(conn, query):
    cus = conn.cursor()
    cus.execute(query)
    columns = [column[0] for column in cus.description]
    types = [column[1] for column in cus.description]
    rows = cus.fetchall()

    data = pd.DataFrame.from_records(rows, columns=columns)
    for col in data.columns:
        if all(isinstance(val, Decimal) for val in data[col]):
            data[col] = data[col].astype(float)
            
    return data

def load_function(func_name: str):
    """
    Dynamically loads and returns a function object from a string reference.
    Example func_name: 'etl.post_processing.users_post_process'
    """
    module_name, function_name = func_name.rsplit('.', 1)
    module = importlib.import_module(module_name)
    return getattr(module, function_name)

def file_write(x, filepath):
    fp = open(filepath, 'w')
    fp.write(x)
    fp.close()

def infer_sql_type(dtype):
    """
    Given a pandas dtype (like 'int64', 'float64', 'object', 'datetime64[ns]'),
    return a string representing a simple corresponding Netezza SQL column type.
    Adjust as needed for your environment.
    """
    if "int" in str(dtype).lower():
        return "INT"
    elif "float" in str(dtype).lower() or "double" in str(dtype).lower():
        return "FLOAT"
    elif "datetime" in str(dtype).lower():
        return "DATE"
    # Fallback for objects or strings
    return "VARCHAR(255)"

def generate_create_table_sql(df, table_name):
    """
    Dynamically generate a CREATE TABLE statement for Netezza based on a DataFrame's columns and dtypes.
    Example result:
        CREATE TABLE my_table (
            col1 INT,
            col2 VARCHAR(255),
            col3 TIMESTAMP
        )
    """
    cols_defs = []
    for col in df.columns:
        col_type = infer_sql_type(df[col].dtype)
        col_def = f"{col} {col_type}"
        cols_defs.append(col_def)

    cols_str = ",\n  ".join(cols_defs)
    create_stmt = f"CREATE TABLE {table_name} (\n  {cols_str}\n)"
    return create_stmt

def generate_create_ext_table_sql(df, table_name, file_path):
    """
    Dynamically generate a CREATE TABLE statement for Netezza based on a DataFrame's columns and dtypes.
    Example result:
        CREATE TABLE my_table (
            col1 INT,
            col2 VARCHAR(255),
            col3 TIMESTAMP
        )
    """
    cols_defs = []
    for col in df.columns:
        col_type = infer_sql_type(df[col].dtype)
        col_def = f"{col} {col_type}"
        cols_defs.append(col_def)

    cols_str = ",\n  ".join(cols_defs)
    create_stmt = f"""
    CREATE EXTERNAL TABLE {table_name}_EXT (
        {cols_str}
    )
    USING (
        DATAOBJECT ('{file_path}')
        DELIMITER ','
        SKIPROWS 1
        REMOTESOURCE 'ODBC'
    )
    """
    return create_stmt

def db_write(df, table_name, batch_size=10_000):
    """
    Overwrite (replace) an existing table in Netezza with the contents of df
    using row-by-row inserts in batches. Utilizes fast_executemany for efficiency.

    :param conn: pyodbc connection object to Netezza
    :param df: pandas DataFrame to insert
    :param table_name: name of the table to overwrite
    :param batch_size: number of rows per batch (tune this for performance)
    """
    logger.debug('Starting to write df to db')
    conn = db_conn()
    df_path = 'tmp.csv'

    # 1. Drop existing table
    drop_sql = f"""
    DROP TABLE {table_name}     IF EXISTS;
    DROP TABLE {table_name}_EXT IF EXISTS;
    """
    cursor = conn.cursor()
    cursor.execute(drop_sql)
    conn.commit()

    # 2. Create new table dynamically
    sql = generate_create_table_sql(df, table_name)
    file_write(sql, 'tmp/create.sql')
    cursor.execute(sql)
    conn.commit()
    logger.debug('Create DDL Executed')

    # 3. Save df to a file
    df.to_csv(df_path, index=False, header=True)
    df_path = os.path.abspath(df_path)
    logger.debug('Saved to CSV')

    # 4. Create an external table pointing to the CSV
    sql = generate_create_ext_table_sql(df, table_name, df_path)
    file_write(sql, 'tmp/create_ext.sql')
    cursor.execute(sql)
    logger.debug('Create External Table DDL Executed')
    
    # 5. Load data from the external table into the main table
    cursor.execute(f"INSERT INTO {table_name} SELECT * FROM {table_name}_EXT")
    logger.debug('Finished Insert')
    conn.commit()
    logger.debug('Commited Insert')

    # 6. Clean up
    os.remove(df_path)
    cursor.execute(f"DROP TABLE {table_name}_EXT IF EXISTS")
    conn.commit()
    logger.debug('Cleaned up')

    # Close connection
    cursor.close()
    conn.close()