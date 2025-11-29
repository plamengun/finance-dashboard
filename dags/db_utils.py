import clickhouse_connect
from airflow.hooks.base import BaseHook
from typing import Optional
from airflow.utils.log.logging_mixin import LoggingMixin

log = LoggingMixin().log


def get_clickhouse_client(conn_id: str = "clickhouse_default"):
    """
    Get ClickHouse client from Airflow connection.
    
    Args:
        conn_id (Optional): Airflow connection ID for ClickHouse
        
    Returns:
        ClickHouse client instance
        
    Raises:
        ValueError: If connection is not found or invalid
    """
    try:
        # Get connection from Airflow
        conn = BaseHook.get_connection(conn_id)
        extra = conn.extra_dejson

        host = extra.get("host", "localhost")
        port = int(extra.get("port", 8123))
        username = extra.get("username", "default")
        password = extra.get("password", "")
        database = extra.get("database", "default")
        secure = extra.get("secure", False)

        try:
            client_connection = clickhouse_connect.get_client(
                host=host,
                port=port,
                username=username,
                password=password,
                database=database,
                secure=secure,
            )
            result = client_connection.query("SELECT 1")
            print("✅ Success! Result:", result.result_rows)
        except Exception as e:
            raise ValueError(f"❌ Failed to connect to ClickHouse: {e}")
        return client_connection
    except Exception as e:
        raise ValueError(f"❌ Failed to create ClickHouse client from connection '{conn_id}': {e}")


def test_clickhouse_connection(conn_id: str = "clickhouse_default") -> bool:
    """
    Test ClickHouse connection.
    
    Args (Optional):
        conn_id: Airflow connection ID for ClickHouse
        
    Returns:
        True if connection successful, False otherwise
    """
    try:
        client = get_clickhouse_client(conn_id)
        # Test with a simple query
        result = client.query("SELECT 1")
        log.info(f"✅ ClickHouse test succeeded: {result.result_rows}")
        client.close()
        return True
    except Exception as e:
        print(f"❌ ClickHouse connection test failed: {e}")
        return False


def execute_clickhouse_query(client_connection, query: str, parameters: Optional[dict] = None):
    """
    Execute a ClickHouse query.
    
    Args:
        client_connection: ClickHouse client connection
        query: SQL query to execute
        parameters (Optional): Query parameters
        
    Returns:
        Query result
    """
    try:    
        if parameters:
            result = client_connection.query(query, parameters=parameters)
        else:
            result = client_connection.query(query)
        return result
    except Exception as e:
        raise ValueError(f"❌ Failed to execute ClickHouse query: {e}")
