from airflow.decorators import dag, task
from airflow.models import Variable
import requests
from datetime import datetime
from db_utils import get_clickhouse_client, execute_clickhouse_query



    
@task
def get_eodhd_companies():
    """Get companies from EODHD API using Airflow variable"""
    try:
        # Get API key from Airflow variable
        api_key = Variable.get("EODHD_API_KEY")
        
        if not api_key:
            raise ValueError("EODHD_API_KEY variable not found in Airflow")
        
        response = requests.get(f'https://eodhd.com/api/exchange-symbol-list/NASDAQ?api_token={api_key}&fmt=json')
        response.raise_for_status()
        return response.json()

    except Exception as e:
        print(f"Error fetching EODHD data: {e}")
        raise

@task
def store_companies_in_clickhouse(companies_data):
    """Store companies data in ClickHouse database"""
    try:
        if not companies_data:
            print("No companies data to store")
            return 0
            
        # Create table if it doesn't exist
        create_table_query = """
        CREATE TABLE IF NOT EXISTS nasdaq_companies (
            symbol String,
            name String,
            country String,
            exchange String,
            type String,
            currency String,
            isin String,
            ingest_date Date
        ) ENGINE = ReplacingMergeTree(ingest_date)
        ORDER BY (symbol)
        """

        client=get_clickhouse_client(conn_id="clickhouse_default")
        res = execute_clickhouse_query(client, create_table_query)
        print(list(res.named_results()))
        
        # Prepare data for insertion
        insert_data = []
        for company in companies_data:
            insert_data.append([
                company.get('Code', ''),
                company.get('Name', ''),
                company.get('Country', ''),
                company.get('Exchange', 'NASDAQ'),
                company.get('Type', ''),
                company.get('Currency', ''),
                company.get('ISIN', ''),
                datetime.now().date()
            ])
        
        # Insert data
        if insert_data:
            # Using batch insert for better performance
            client = get_clickhouse_client("clickhouse_default")
            try:
                client.insert("nasdaq_companies", insert_data, column_names=[
                    'symbol',
                    'name',
                    'country',
                    'exchange',
                    'type',
                    'currency',
                    'isin',
                    'ingest_date'
                ])
                print(f"Successfully stored {len(insert_data)} companies in ClickHouse")
                return len(insert_data)
            finally:
                client.close()
        return 0
    except Exception as e:
        print(f"Error storing data in ClickHouse: {e}")
        raise

@task
def process_companies(companies_data):
    """Process the companies data"""
    if companies_data:
        print(f"Retrieved {len(companies_data)} companies from NASDAQ")
        return len(companies_data)
    return 0

# Execute the task pipeline
# companies_data = get_eodhd_companies()
# companies_count = process_companies(companies_data)
# stored_count = store_companies_in_clickhouse(companies_data)

# return {
#     'retrieved_count': companies_count,
#     'stored_count': stored_count
# }
