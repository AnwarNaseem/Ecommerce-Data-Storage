from logging import exception
from sqlite3 import Cursor
import mysql.connector

# Global methods to push interact with the Database
# This method establishes the connection with the MySQL

def get_connection():
    connection = mysql.connector.connect(
            user = 'root',
            password = 'Schemaa@5632',
            host = '127.0.0.1',
            database = 'ecommerce_record')
    cursor = connection.cursor()
    return connection

def create_server_connection(connection):
    # Implement the logic to create the server connection
    if connection is None:
        try:
            connection = get_connection()
            cursor = connection.cursor()
            print(" My SQL database connection successful")
        except Exception as err:
            print(f"Error:'{err}'")       
    return connection
    
# This method will create the database and make it an active database
def create_and_switch_database(connection, db_name, switch_db):
    connection = get_connection()
    try:
        cursor = connection.cursor()
        drop_query = "DROP DATABASE IF EXISTS " + db_name
        db_query = " CREATE DATABASE " + db_name
        switch_query = " USE " + switch_db
        cursor.execute(drop_query)
        cursor.execute(db_query)
        cursor.execute(switch_query)
        print(" Database created successfully")
    except Exception as err:
        print("Error in creating database: '{err}'")
        

# Use this function to create the tables in a database
def create_table(connection, table_creation_statement):
    connection = get_connection()
    cursor = connection.cursor()
    try:
        cursor.execute(table_creation_statement)
        connection.commit()
        print("Table creation successful")
    except Exception as err:
        print("Error in table creation: '{err}'")


# Perform all single insert statments in the specific table through a single function call
def create_insert_query(connection, query):
    # This method will perform creation of the table
    # this can also be used to perform single data point insertion in the desired table
    connection = get_connection()
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        connection.commit()
        print("Insert operation successful")
    except Exception as err:
        print("Error in insert query:'{err}'")

# retrieving the data from the table based on the given query
def select_query(connection, query):
    # fetching the data points from the table
    connection = get_connection()
    cursor = connection.cursor()
    result = None
    try:
        cursor.execute(query)
        result = cursor.fetchall()
        return result
    except Exception as err:
        print("Error in  select query : '{err}'")

# Execute multiple insert statements in a table
def insert_many_records(connection, sql, values):
    connection = get_connection()
    cursor = connection.cursor()
    try:
        cursor.execute(sql,values)
        connection.commit()
        print("Insert operation successful")
    except Exception as err:
        print("Error in insert query:'{err}'")
 
def close_connection(connection):
    if connection:
        connection.close()