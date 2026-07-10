import os
import snowflake.connector
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.serialization import load_pem_private_key
from snowflake.connector import ProgrammingError
from cryptography.hazmat.backends import default_backend

SF_ACCOUNT = os.getenv("SF_ACCOUNT")
SF_USER = os.getenv("SF_USER")

def _load_private_key():
    """Load and return the DER-encoded private key bytes for Snowflake auth."""

    pem_data = os.getenv("SF_PRIVATE_KEY").encode()

    private_key = load_pem_private_key(pem_data, password=None, backend=default_backend())

    return private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )

def create_sf_connection(warehouse, database, schema, role):
    try:
        # Establish the connection
        connection = snowflake.connector.connect(
            user=SF_USER,
            private_key=_load_private_key(),
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