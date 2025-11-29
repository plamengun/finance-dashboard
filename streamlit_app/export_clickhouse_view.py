# export_clickhouse_view.py
import clickhouse_connect
import pandas as pd

client = clickhouse_connect.get_client(
    host="localhost",
    port=8123,
    username="default",
    password=""
)

df = client.query_df("SELECT * FROM financial_data.company_info_final")
df.to_parquet("dashboard_data.parquet", engine="pyarrow", index=False)

print("✅ Exported to Parquet")
