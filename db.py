import os
import socket
from dotenv import load_dotenv
from databricks import sql

# Load environment variables from a .env file
load_dotenv()

def get_db_connection():
    """
    Establishes and returns a connection to the Databricks SQL Warehouse.

    Safety checks:
    - Validates required env vars.
    - Refuses to connect if the server hostname is localhost/127.0.0.1 (common misconfiguration when deploying to Render).
    - Optionally sets a global socket timeout if DATABRICKS_SOCKET_TIMEOUT is set (seconds).
    """

    server_hostname = os.getenv("DATABRICKS_SERVER_HOSTNAME")
    http_path = os.getenv("DATABRICKS_HTTP_PATH")
    access_token = os.getenv("DATABRICKS_TOKEN")

    if not server_hostname or not http_path or not access_token:
        raise RuntimeError(
            "Missing Databricks connection environment variables. "
            "Set DATABRICKS_SERVER_HOSTNAME, DATABRICKS_HTTP_PATH and DATABRICKS_TOKEN."
        )

    # Prevent accidental local-only hostname in hosted environment (Render doesn't have local proxy)
    if server_hostname in ("127.0.0.1", "localhost"):
        raise RuntimeError(
            "DATABRICKS_SERVER_HOSTNAME is set to localhost (127.0.0.1). "
            "On Render you must use the Databricks workspace server hostname (the public server hostname) "
            "or configure a proper reachable proxy/tunnel. Do NOT use localhost."
        )

    # Optional: set a global socket timeout (in seconds) to avoid indefinite blocking on socket operations.
    sock_timeout = os.getenv("DATABRICKS_SOCKET_TIMEOUT")
    if sock_timeout:
        try:
            sock_to = float(sock_timeout)
            # This sets default timeout for new socket objects (applies to blocking socket ops).
            socket.setdefaulttimeout(sock_to)
        except Exception:
            # If parsing fails, ignore and continue with default behavior.
            pass

    return sql.connect(
        server_hostname=server_hostname,
        http_path=http_path,
        access_token=access_token
    )
