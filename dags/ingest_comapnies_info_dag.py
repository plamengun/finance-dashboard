from airflow.decorators import dag, task
import json
import uuid
import pandas as pd
import time
import requests
import yfinance as yf
import logging
from datetime import datetime, timezone
from confluent_kafka import Producer, Consumer, TopicPartition, KafkaError, KafkaException
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.models import Variable
from db_utils import get_clickhouse_client, execute_clickhouse_query
from ingest_market_symbols_to_db_dag import get_eodhd_companies, store_companies_in_clickhouse, process_companies
from ingest_market_symbols_to_kafka_dag import get_unique_company_symbols
from ingest_companies_info_to_kafka_dag import process_company_symbols



@dag(
    dag_id='ingest_companies_info_dag',
    schedule='@daily',
    catchup=False
)
def ingest_market_symbols_to_db_dag():
    """Ingest market mapping from EODHD APIs and store in ClickHouse"""
    
    companies_data = get_eodhd_companies()
    companies_count = process_companies(companies_data)
    stored_count = store_companies_in_clickhouse(companies_data)

    company_symbols = get_unique_company_symbols()

    processed_company_symbols = process_company_symbols(company_symbols)

    

    

dag_instance = ingest_market_symbols_to_db_dag()

