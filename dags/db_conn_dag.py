from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from datetime import datetime
import clickhouse_connect

@dag(
    dag_id="clickhouse_conn_dag",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
)
def clickhouse_conn_dag():

    @task
    def test_clickhouse_connection():
        conn = BaseHook.get_connection("clickhouse_default")
        extra = conn.extra_dejson

        host = extra.get("host", "localhost")
        port = int(extra.get("port", 8123))
        username = extra.get("username", "default")
        password = extra.get("password", "")
        database = extra.get("database", "default")
        secure = extra.get("secure", False)

        try:
            client = clickhouse_connect.get_client(
                host=host,
                port=port,
                username=username,
                password=password,
                database=database,
                secure=secure,
            )
            result = client.query("SELECT 1")
            print("✅ Success! Result:", result.result_rows)
        except Exception as e:
            raise ValueError(f"❌ Failed to connect to ClickHouse: {e}")

    test_clickhouse_connection()

dag_instance = clickhouse_conn_dag()