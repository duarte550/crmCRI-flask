
import os
from dotenv import load_dotenv
from databricks import sql

# Load environment variables from a .env file
load_dotenv()

def get_db_connection():
    """
    Establishes and returns a connection to the Databricks SQL Warehouse.
    """
    server_hostname = os.getenv("DATABRICKS_SERVER_HOSTNAME")
    if not server_hostname:
        raise ValueError("Environment variables for Databricks are missing.")
        
    return sql.connect(
        server_hostname=server_hostname,
        http_path=os.getenv("DATABRICKS_HTTP_PATH"),
        access_token=os.getenv("DATABRICKS_TOKEN")
    )
