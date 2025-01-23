import os
import snowflake.connector
from snowflake.connector import ProgrammingError

SF_ACCOUNT = os.getenv("SF_ACCOUNT")
SF_USER = os.getenv("SF_USER")
SF_PASSWORD = os.getenv("SF_PASSWORD")

def create_sf_connection(warehouse, database, schema, role):
    try:
        # Establish the connection
        connection = snowflake.connector.connect(
            user=SF_USER,
            password=SF_PASSWORD,
            account=SF_ACCOUNT,
            warehouse=warehouse,
            database=database,
            schema=schema,
            role=role
        )
        print("Connection to Snowflake established successfully!")
        return connection
    except ProgrammingError as e:
        print(f"Error establishing connection: {e}")
        return None

def close_sf_connection(connection):
    #Closing the connection
    if connection:
        try:
            connection.close()
            print("Connection to Snowflake closed successfully.")
        except Exception as e:
            print(f"Error closing connection: {e}")
    else:
        print("No active connection to close.")