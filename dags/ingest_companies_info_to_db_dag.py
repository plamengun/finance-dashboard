from airflow.decorators import dag, task
import json
import time
from datetime import datetime
from db_utils import get_clickhouse_client, execute_clickhouse_query
from confluent_kafka import Consumer, TopicPartition, KafkaError, KafkaException
import pandas as pd

asd
@dag(
    dag_id='ingest_companies_info_to_db_dag',
    schedule='@daily',
    catchup=False
)
def ingest_companies_info_to_db_dag():
    """Ingest companies info to ClickHouse"""

    @task
    def get_company_info_from_kafka():
        consumer = Consumer({
            'bootstrap.servers': 'host.containers.internal:9092',
            'group.id': 'company_info_results',
            'enable.auto.commit': False
        })

        topic = 'company_info_results'
        partition = 0
        tp = TopicPartition(topic, partition)

        consumer.assign([tp])

        # Force assignment to complete with timeout
        assignment_timeout = 10  # 10 seconds timeout
        start_time = time.time()
        while time.time() - start_time < assignment_timeout:
            msg = consumer.poll(1.0)  # wait up to 1s
            if msg is None:
                # assignment is now active, can safely seek
                break
            elif msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    break
                else:
                    raise KafkaException(msg.error())
            else:
                # got a real message, rewind after seeking
                break
        else:
            raise TimeoutError("Failed to complete consumer assignment within timeout")

        _, high_offset = consumer.get_watermark_offsets(tp)

        ts_7_days_ago = int((time.time() - 7 * 86400) * 1000)
        tp_ts = TopicPartition(topic, partition, ts_7_days_ago)
        seek_tp = consumer.offsets_for_times([tp_ts])[0]

        if seek_tp and seek_tp.offset != -1:
            consumer.seek(seek_tp)
            start_offset = seek_tp.offset
            print(f"📍 Seeking from 7 days ago at offset {start_offset}")
        else:
            print("⚠️  No offset found for 7-day timestamp. Using beginning of topic.")
            consumer.seek(TopicPartition(topic, partition, 0))
            start_offset = 0

        # Process all available messages with timeout to prevent infinite processing
        max_processing_time = 300  # 5 minutes timeout
        messages_processed = 0
        all_records = []
        start_processing_time = time.time()
        consecutive_empty_polls = 0
        max_empty_polls = 10  # Stop after 10 consecutive empty polls
        
        print(f"🚀 Starting to process messages from offset {start_offset} to {high_offset}")
        
        # Check if there are any messages to process
        if start_offset >= high_offset:
            print("⚠️  No new messages to process (start_offset >= high_offset)")
            consumer.close()
            return []
        
        while time.time() - start_processing_time < max_processing_time:
            msg = consumer.poll(1.0)
            if msg is None:
                consecutive_empty_polls += 1
                if consecutive_empty_polls >= max_empty_polls:
                    print(f"🛑 No messages received for {max_empty_polls} consecutive polls. Stopping.")
                    break
                continue
            
            # Reset empty poll counter when we get a message
            consecutive_empty_polls = 0
            
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition reached
                    print("📄 Reached end of partition")
                    break
                else:
                    print(f"❌ Consumer error: {msg.error()}")
                    continue

            try:
                company_info = json.loads(msg.value().decode("utf-8"))
                symbol = company_info.get("symbol", "")
                data = company_info.get("data", {})
                fetched_at = company_info.get("fetched_at", "")
                batch_id = company_info.get("batch_id", "")

                print(f"🔍 Processing message {messages_processed + 1} for symbol: {symbol}")
                print(f"🔍 Data type: {type(data)}")
                print(f"🔍 Data keys: {list(data.keys()) if isinstance(data, dict) else 'Not a dict'}")

                # Skip if data is not a dictionary (e.g., error messages)
                if not isinstance(data, dict):
                    print(f"⚠️  Skipping message for {symbol}: data is not a dictionary")
                    messages_processed += 1
                    continue

                # Flatten "data" into tabular rows
                print(f"📅 Flattening data for {symbol}: {len(data)} date entries")
                for date, metrics in data.items():
                    flat_record = {
                        "symbol": symbol,
                        "date": date,
                        "fetched_at": fetched_at,
                        "batch_id": batch_id
                    }
                    # Add financial metrics
                    if isinstance(metrics, dict):
                        flat_record.update(metrics)
                        print(f"  📊 Added metrics for {date}: {list(metrics.keys())}")
                    else:
                        print(f"  ⚠️  Metrics for {date} is not a dict: {type(metrics)}")
                    all_records.append(flat_record)

                messages_processed += 1
                
                # Log progress every 10 messages
                if messages_processed % 10 == 0:
                    print(f"📈 Progress: {messages_processed} messages processed, {len(all_records)} records collected")

            except Exception as e:
                print(f"❌ Error processing message: {e}. Skipping...")
                messages_processed += 1
                continue

        consumer.close()
        
        print(f"📊 Total messages processed: {messages_processed}")
        print(f"📊 Total records collected: {len(all_records)}")
        
        if all_records:
            print(f"✅ Processed {len(all_records)} records from {messages_processed} messages. {all_records}")
            return all_records
        else:
            raise ValueError("No records to process! Stopping the pipeline.")
            

    @task
    def persist_to_db(all_records):
        """Persist company financial data to ClickHouse database"""
        
        print(f"🔍 Final DataFrame: {all_records}")
        # Handle None or empty DataFrame
        if all_records is None:
            print("No data received from previous task")
            return
            
        
        conn_id = "clickhouse_default"
        
        # Create table if not exists
        create_table_query = """
        CREATE TABLE IF NOT EXISTS financial_data.company_info (
            symbol String,
            date String,
            fetched_at String,
            batch_id String,
            `Total Revenue` Nullable(Float64),
            `Net Income` Nullable(Float64),
            `Operating Income` Nullable(Float64),
            `Gross Profit` Nullable(Float64)
        ) ENGINE = ReplacingMergeTree()
        ORDER BY (symbol, date)
        """
        # drop_table_query = """
        # DROP TABLE IF EXISTS financial_data.company_info
        # """
        try:
            # Create table

            # execute_clickhouse_query(conn_id, drop_table_query)
            # print("✅ Table dropped successfully")

            execute_clickhouse_query(conn_id, create_table_query)
            print("✅ Table created/verified successfully")
            
            # Prepare data for insertion
            # Convert DataFrame to list of dictionaries for easier processing
            
            # Insert data in batches
            batch_size = 100
            total_inserted = 0
            
            for i in range(0, len(all_records), batch_size):
                batch = all_records[i:i + batch_size]
                
                # Build INSERT query for this batch
                insert_query = """
                INSERT INTO financial_data.company_info 
                (symbol, date, fetched_at, batch_id, `Total Revenue`, `Net Income`, `Operating Income`, `Gross Profit`)
                VALUES
                """
                
                values_list = []
                for record in batch:
                    # Handle None values and ensure proper formatting
                    total_revenue = record.get('Total Revenue')
                    net_income = record.get('Net Income')
                    operating_income = record.get('Operating Income')
                    gross_profit = record.get('Gross Profit')
                    
                    # Convert to proper format for ClickHouse
                    values = [
                        f"'{record.get('symbol', '')}'",
                        f"'{record.get('date', '')}'",
                        f"'{record.get('fetched_at', '')}'",
                        f"'{record.get('batch_id', '')}'",
                        str(total_revenue) if total_revenue is not None else 'NULL',
                        str(net_income) if net_income is not None else 'NULL',
                        str(operating_income) if operating_income is not None else 'NULL',
                        str(gross_profit) if gross_profit is not None else 'NULL'
                    ]
                    values_list.append(f"({', '.join(values)})")
                
                insert_query += ', '.join(values_list)
                
                # Execute batch insert
                execute_clickhouse_query(conn_id, insert_query)
                total_inserted += len(batch)
                print(f"✅ Inserted batch {i//batch_size + 1}: {len(batch)} records")
            
            print(f"✅ Successfully persisted {total_inserted} records to ClickHouse")
            
        except Exception as e:
            print(f"❌ Error persisting data to database: {e}")
            raise

    # Execute the pipeline
    company_data = get_company_info_from_kafka()
    persist_to_db(company_data)

dag_instance = ingest_companies_info_to_db_dag()