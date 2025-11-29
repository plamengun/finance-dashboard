from typing import List
from airflow.decorators import dag, task
import json
import time
from datetime import datetime
from airflow.utils.log.logging_mixin import LoggingMixin
from confluent_kafka import Consumer, TopicPartition, Producer, KafkaError, KafkaException
import yfinance as yf
import pandas as pd


KAFKA_BOOTSTRAP_SERVERS = 'host.containers.internal:9092'
log = LoggingMixin().log

def get_kafka_producer():
    return Producer({'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS})

@task  
def process_company_symbols(company_symbols: List[str]):
    """Process company symbols by fetching quarterly income statement from each company from yfinance. Produces the results to Kafka"""
    producer = get_kafka_producer()
    for symbol in company_symbols:
        try:
            # Get company info using yfinance
            ticker = yf.Ticker(symbol)
            df = ticker.quarterly_income_stmt
    
            # Check if quarterly income statement exists
            if df is not None and not df.empty:
                wanted = ['Total Revenue', 'Net Income', 'Operating Income', 'Gross Profit']
                # Only include columns that exist in the DataFrame
                available_columns = [col for col in wanted if col in df.index]
        
                if available_columns:
                    subset = df.loc[available_columns]
                    result = {}
            
                    for col in subset.columns:
                        # Handle different column types (datetime, string, etc.)
                        if hasattr(col, 'strftime'):
                            col_key = col.strftime("%Y-%m-%d")
                        else:
                            col_key = str(col)
                
                        result[col_key] = {}
                        for idx, val in subset[col].items():
                            # Proper null checking
                            if pd.notna(val):
                                result[col_key][idx] = val
                            else:
                                result[col_key][idx] = None
                else:
                    result = "No financial metrics available"
            else:
                result = "No quarterly income statement available"

            payload = {
                "symbol": symbol,
                "data": result,
                "fetched_at": datetime.now().isoformat()
            }

            #Produce to Kafka
            producer.produce(
                topic='company_info_results',
                value=json.dumps(payload).encode('utf-8')
            )
        except Exception as e:
            log.info(f"Error processing company symbol: {e}")
            continue
    producer.flush()

# @dag(
#     dag_id='ingest_companies_info_to_kafka_dag',
#     schedule='@daily',
#     catchup=False
# )
# def ingest_companies_info_to_kafka_dag():
#     """Ingest companies info to Kafka"""

#     @task(do_xcom_push=False)
#     def process_batches():
#         consumer = Consumer({
#             'bootstrap.servers': 'host.containers.internal:9092',
#             'group.id': 'company_info_processor',
#             'enable.auto.commit': False
#         })

#         producer = Producer({'bootstrap.servers': 'host.containers.internal:9092'})

#         topic = 'market_mapping'
#         partition = 0
#         tp = TopicPartition(topic, partition)

#         consumer.assign([tp])

#         # Force assignment to complete with timeout
#         assignment_timeout = 10  # 10 seconds timeout
#         start_time = time.time()
#         while time.time() - start_time < assignment_timeout:
#             msg = consumer.poll(1.0)  # wait up to 1s
#             if msg is None:
#                 # assignment is now active, can safely seek
#                 break
#             elif msg.error():
#                 if msg.error().code() == KafkaError._PARTITION_EOF:
#                     break
#                 else:
#                     raise KafkaException(msg.error())
#             else:
#                 # got a real message, rewind after seeking
#                 break
#         else:
#             raise TimeoutError("Failed to complete consumer assignment within timeout")

#         _, high_offset = consumer.get_watermark_offsets(tp)

#         ts_3_days_ago = int((time.time() - 3 * 86400) * 1000)
#         tp_ts = TopicPartition(topic, partition, ts_3_days_ago)
#         seek_tp = consumer.offsets_for_times([tp_ts])[0]

#         if seek_tp and seek_tp.offset != -1:
#             consumer.seek(seek_tp)
#             start_offset = seek_tp.offset
#         else:
#             print("No offset found for timestamp. Using beginning.")
#             consumer.seek(TopicPartition(topic, partition, 0))
#             start_offset = 0

#         # Process messages with a reasonable limit
#         max_messages = 1000  # Limit to prevent infinite processing
#         messages_processed = 0
        
#         while messages_processed < max_messages:
#             msg = consumer.poll(1.0)
#             if msg is None:
#                 # No more messages available
#                 break
#             if msg.error():
#                 if msg.error().code() == KafkaError._PARTITION_EOF:
#                     # End of partition reached
#                     break
#                 else:
#                     print(f"Consumer error: {msg.error()}")
#                     continue

#             try:
#                 batch = json.loads(msg.value().decode("utf-8"))
#                 batch_id = batch.get("batch_id")
#                 symbols = batch.get("entries", [])

#                 for symbol in symbols:
#                     try:
#                         # Get company info using yfinance
#                         ticker = yf.Ticker(symbol)
#                         df = ticker.quarterly_income_stmt
                        
#                         # Check if quarterly income statement exists
#                         if df is not None and not df.empty:
#                             wanted = ['Total Revenue', 'Net Income', 'Operating Income', 'Gross Profit']
#                             # Only include columns that exist in the DataFrame
#                             available_columns = [col for col in wanted if col in df.index]
                            
#                             if available_columns:
#                                 subset = df.loc[available_columns]
#                                 result = {}
                                
#                                 for col in subset.columns:
#                                     # Handle different column types (datetime, string, etc.)
#                                     if hasattr(col, 'strftime'):
#                                         col_key = col.strftime("%Y-%m-%d")
#                                     else:
#                                         col_key = str(col)
                                    
#                                     result[col_key] = {}
#                                     for idx, val in subset[col].items():
#                                         # Proper null checking
#                                         if pd.notna(val):
#                                             result[col_key][idx] = val
#                                         else:
#                                             result[col_key][idx] = None
#                             else:
#                                 result = "No financial metrics available"
#                         else:
#                             result = "No quarterly income statement available"

#                         payload = {
#                             "symbol": symbol,
#                             "data": result,
#                             "fetched_at": datetime.now().isoformat(),
#                             "batch_id": batch_id
#                         }

#                         producer.produce(
#                             topic='company_info_results',
#                             value=json.dumps(payload).encode('utf-8')
#                         )

#                     except Exception as e:
#                         fail_payload = {
#                             "symbol": symbol,
#                             "error": str(e),
#                             "batch_id": batch_id,
#                             "failed_at": datetime.now().isoformat()
#                         }

#                         producer.produce(
#                             topic='company_info_failed',
#                             value=json.dumps(fail_payload).encode('utf-8')
#                         )

#                 messages_processed += 1

#             except Exception as e:
#                 print(f"Error processing batch: {e}")
#                 continue

#         producer.flush()
#         consumer.close()
#         print(f"✅ Finished processing {messages_processed} batches.")

#     process_batches()

# dag_instance = ingest_companies_info_to_kafka_dag()
    
#     @task
#     def get_companies_symbols_from_kafka():
#         """Get companies symbols from Kafka"""
#         consumer = Consumer({'bootstrap.servers': 'host.containers.internal:9092', 
#                             'group.id': 'companies_info_consumer',
#                             'enable.auto.commit': False
#                             })

#         topic = 'market_mapping'
#         partition = 0
#         consumer.assign([TopicPartition(topic, partition)])

#         # 3 days ago in ms
#         ts_3_days_ago = int((time.time() - 3 * 86400) * 1000)

#         # Find the offset for 3-days-ago timestamp
#         tp = TopicPartition(topic, partition, ts_3_days_ago)
#         offset = consumer.offsets_for_times([tp])[0]
#         if offset and offset.offset != -1:
#             consumer.seek(offset)
#         else:
#             print("No offset found for timestamp. Using beginning.")
#             consumer.seek(TopicPartition(topic, partition, 0))

#         batches_received = 0

#         batch_symbols = []
#         for _ in range(10):
#             msg = consumer.poll(1.0)
            
#             if msg is None:
#                 print('Waiting for message...')
#                 continue
#             if msg.error():
#                 print("Consumer error: {}".format(msg.error()))
#                 continue
#             print("Received message: {}".format(msg.value().decode('utf-8')))
#             batch_symbols.append(json.loads(msg.value().decode("utf-8")))
#         consumer.close()
#         print(f"Total symbols received: {len(batch_symbols)}")
#         return batch_symbols

#     get_companies_symbols_from_kafka()

# dag_instance = ingest_companies_info_to_kafka_dag()




