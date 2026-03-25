
import os
from dotenv import load_dotenv
from databricks import sql

# Load environment variables from a .env file
load_dotenv()

def get_db_connection():
    """
    Establishes and returns a connection to the Databricks SQL Warehouse.
    """
    return sql.connect(
        server_hostname=os.getenv("DATABRICKS_SERVER_HOSTNAME"),
        http_path=os.getenv("DATABRICKS_HTTP_PATH"),
        access_token=os.getenv("DATABRICKS_TOKEN")
    )
