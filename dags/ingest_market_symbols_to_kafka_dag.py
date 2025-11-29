from airflow.decorators import dag, task
import json
import uuid
from datetime import datetime, timezone
from confluent_kafka import Producer
from airflow.utils.log.logging_mixin import LoggingMixin
from db_utils import get_clickhouse_client, execute_clickhouse_query


conf = {'bootstrap.servers': 'host.containers.internal:9092',
        'client.id': f'airflow-producer-{uuid.uuid4()}'}
log = LoggingMixin().log




@task
def get_unique_company_symbols():
    """Get unique company symbols from ClickHouse"""
    clickhouse_client = get_clickhouse_client()
    query = "SELECT symbol FROM financial_data.nasdaq_companies"
    
    rows = execute_clickhouse_query(clickhouse_client, query).result_rows
    
    seen = set()
    result = []
    for row in rows:
        value = row[0]
        if value not in seen:
            seen.add(value)
            result.append(value)
    return result




# @task
# def segment_company_symbols(company_symbols):
#     """Segment company symbols into chunks of 1000"""
#     return [company_symbols[i:i+1000] for i in range(0, len(company_symbols), 1000)]


# @task
# def load_to_kafka(company_symbols_chunks):
#     """Stream company symbols to Kafka"""

#     producer = Producer(conf)

#     batch_id = f"group-{uuid.uuid4()}"

#     for idx, chunk in enumerate(company_symbols_chunks):
#         payload = {
#             "batch_id": batch_id,
#             "chunk_index": idx,
#             "total_chunks": len(company_symbols_chunks),
#             "entries": chunk,
#             "posted_at": datetime.now(timezone.utc).isoformat()
#         }

#         try:
#             producer.produce(topic="market_mapping", value=json.dumps(payload).encode('utf-8'))
#             log.info(f"Sent {len(chunk)} symbols to Kafka. Chunk index: {idx}; Total chunks: {len(company_symbols_chunks)}")
#         except Exception as e:
#             log.error(f"Error sending message to Kafka: {e}")
#             raise e
#         producer.flush()

    # DAG dependencies
#     company_symbols = get_unique_company_symbols()
#     company_symbols_chunks = segment_company_symbols(company_symbols)
#     load_to_kafka(company_symbols_chunks)

# dag_instance = ingest_market_symbols_to_kafka_dag()