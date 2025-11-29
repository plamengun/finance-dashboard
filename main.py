
import requests
import json
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv

# producer = KafkaProducer(bootstrap_servers='localhost:9092')

# producer.send('Income_statements', b'Hello, World!')

# producer.flush()


load_dotenv()  # Load from .env file

# api_url = 'https://financialmodelingprep.com/stable/earnings'
# api_url = 'https://financialmodelingprep.com/stable/ratios'
# api_key = os.getenv('FMP_API_KEY')
# params = {
#     'symbol': 'AAPL',
#     'apikey': api_key,
#     'limit': 4,
#     'period': 'annual'
# }

# headers = {
#     'Accept': 'application/json'
# }

# def fetch_api_data(api_url, params=None, headers=None):
#     """Fetch data from API endpoint"""
#     try:
#         response = requests.get(api_url, params=params, headers=headers)
#         response.raise_for_status()
#         return response.json()
#     except requests.exceptions.RequestException as e:
#         print(f"API request failed: {e}")
#         return None

# # Pretty print the JSON response
# result = fetch_api_data(api_url, params, headers)
# if result:
#     print(json.dumps(result, indent=2, sort_keys=True))
# else:
#     print("No data received")


# api_key = os.getenv('ALPHA_VANTAGE_API_KEY')

# def fetch_financial_data(symbol, api_key, period='annual'):
#     """Fetch financial data from Alpha Vantage with period filtering"""
#     url = f'https://www.alphavantage.co/query?function=INCOME_STATEMENT&symbol={symbol}&apikey={api_key}'
    
#     try:
#         response = requests.get(url)
#         response.raise_for_status()
#         return response.json()
#     except requests.exceptions.RequestException as e:
#         print(f"API request failed: {e}")
#         return None

# def calculate_profit_margin(gross_profit, revenue):
#     """Calculate profit margin percentage"""
#     try:
#         if gross_profit and revenue and float(revenue) > 0:
#             return (float(gross_profit) / float(revenue)) * 100
#         return None
#     except (ValueError, TypeError):
#         return None

# def filter_recent_periods(data, period='annual', periods_back=5):
#     """Filter data to show only recent periods"""
#     if not data:
#         return []
    
#     # Determine which reports to use based on period
#     if period == 'quarterly':
#         reports = data.get('quarterlyReports', [])
#         period_name = 'quarterly'
#     else:
#         reports = data.get('annualReports', [])
#         period_name = 'annual'
    
#     if not reports:
#         return []
    
#     # Calculate the cutoff date
#     cutoff_date = datetime.now() - timedelta(days=periods_back * 365)
    
#     filtered_reports = []
    
#     for report in reports:
#         try:
#             date_str = report.get('fiscalDateEnding')
#             if date_str:
#                 report_date = datetime.strptime(date_str, '%Y-%m-%d')
                
#                 if report_date >= cutoff_date:
#                     filtered_reports.append(report)
#         except (ValueError, TypeError):
#             continue
    
#     # Sort by date (most recent first)
#     filtered_reports.sort(key=lambda x: x.get('fiscalDateEnding'), reverse=True)
    
#     return filtered_reports

# def display_profit_summary(data, symbol, period='annual'):
#     """Display Gross Profit and Profit Margin summary"""
#     if not data:
#         print("No financial data available")
#         return
    
#     period_name = "Quarterly" if period == 'quarterly' else "Annual"
#     reports = data.get(f'{period}Reports', [])
    
#     if not reports:
#         print(f"No {period_name.lower()} reports available")
#         return
    
#     print(f"\n📊 {period_name} Financial Summary for {symbol}")
#     print("=" * 70)
#     print(f"{'Period Ending':<15} {'Revenue':<15} {'Gross Profit':<15} {'Profit Margin':<15}")
#     print("-" * 70)
    
#     for i, report in enumerate(reports[:10], 1):  # Show first 10 entries
#         date = report.get('fiscalDateEnding', 'N/A')
#         revenue = report.get('totalRevenue', '0')
#         gross_profit = report.get('grossProfit', '0')
        
#         # Calculate profit margin
#         profit_margin = calculate_profit_margin(gross_profit, revenue)
        
#         # Format the display
#         revenue_display = f"${float(revenue):,.0f}" if revenue != '0' else 'N/A'
#         gross_profit_display = f"${float(gross_profit):,.0f}" if gross_profit != '0' else 'N/A'
#         margin_display = f"{profit_margin:.1f}%" if profit_margin else 'N/A'
        
#         print(f"{date:<15} {revenue_display:<15} {gross_profit_display:<15} {margin_display:<15}")
    
#     if len(reports) > 10:
#         print(f"\n... and {len(reports) - 10} more {period_name.lower()} reports")
    
#     print(f"\nTotal {period_name.lower()} reports available: {len(reports)}")

# def main():
#     """Main function to run the financial analysis"""
#     symbol = 'AAPL'
    
#     print("🔍 Fetching financial data for AAPL...")
#     print("Choose data period:")
#     print("1. Annual reports (last 5 years)")
#     print("2. Quarterly reports (last 5 years)")
#     print("3. Both annual and quarterly")
    
#     choice = input("\nEnter your choice (1, 2, or 3): ").strip()
    
#     # Fetch data
#     result = fetch_financial_data(symbol, api_key)
    
#     if not result:
#         print("❌ Failed to fetch data")
#         return
    
#     if choice == '1':
#         # Annual only
#         annual_data = filter_recent_periods(result, 'annual', 5)
#         display_profit_summary({'annualReports': annual_data}, symbol, 'annual')
        
#         # Save annual data
#         with open(f'{symbol}_annual_profit_data.json', 'w') as f:
#             json.dump(annual_data, f, indent=2)
#         print(f"\n💾 Annual data saved to '{symbol}_annual_profit_data.json'")
        
#     elif choice == '2':
#         # Quarterly only
#         quarterly_data = filter_recent_periods(result, 'quarterly', 5)
#         display_profit_summary({'quarterlyReports': quarterly_data}, symbol, 'quarterly')
        
#         # Save quarterly data
#         with open(f'{symbol}_quarterly_profit_data.json', 'w') as f:
#             json.dump(quarterly_data, f, indent=2)
#         print(f"\n💾 Quarterly data saved to '{symbol}_quarterly_profit_data.json'")
        
#     elif choice == '3':
#         # Both annual and quarterly
#         annual_data = filter_recent_periods(result, 'annual', 5)
#         quarterly_data = filter_recent_periods(result, 'quarterly', 5)
        
#         print("\n" + "="*70)
#         display_profit_summary({'annualReports': annual_data}, symbol, 'annual')
#         print("\n" + "="*70)
#         display_profit_summary({'quarterlyReports': quarterly_data}, symbol, 'quarterly')
        
#         # Save both datasets
#         combined_data = {
#             'annualReports': annual_data,
#             'quarterlyReports': quarterly_data
#         }
#         with open(f'{symbol}_combined_profit_data.json', 'w') as f:
#             json.dump(combined_data, f, indent=2)
#         print(f"\n💾 Combined data saved to '{symbol}_combined_profit_data.json'")
        
#     else:
#         print("❌ Invalid choice. Please run again and select 1, 2, or 3.")

# if __name__ == "__main__":
#     main()



import pandas as pd
import yfinance as yf
dat = yf.Ticker("ABEO")
# df = dat.quarterly_income_stmt
df = dat.get_income_stmt(pretty=True,freq='yearly')
print(df)
# subset = df.loc[['Total Revenue', 'Net Income', 'Operating Income', 'Gross Profit']]

# wanted = ['Total Revenue', 'Net Income', 'Operating Income', 'Gross Profit']
# subset = df.reindex(wanted)
# result = {
#     col.strftime("%Y-%m-%d"): {k: (v if pd.notna(v) else None) for k, v in vals.items()}
#     for col, vals in subset.to_dict().items()
# }

# data_dict = {col.strftime("%Y-%m-%d"): vals for col, vals in subset.to_dict().items()}
# print(data_dict)

#Normalized Net Margin -> 'Net Income' / 'Total Revenue'
#Net Profit Margin -> 'Normalized Income' / 'Total Revenue'
#Operating Profit Margin -> 'Operating Income' / 'Total Revenue'
#GROSS PROFIT MARGIN -> 'Gross Profit' / 'Total Revenue'


# rua = yf.Ticker("^RUA")
# levels = rua.history(period="max")           # DataFrame: Open/High/Low/Close/Volume

# print(levels)


# api_key = os.getenv('EODHD_KEY')
# url = f'https://eodhd.com/api/exchange-symbol-list/NYSE?api_token={api_key}&fmt=json&limit=10'
# data = requests.get(url).json()

# with open('nyse.txt', 'w') as f:
#     f.write(json.dumps(data, indent=2))

# with open('nyse.txt', 'r', encoding="utf-8") as f:

#     data = json.load(f)
#     print(len(data))


# from confluent_kafka import Producer
# import json
# from datetime import datetime
# import uuid

# producer = Producer({'bootstrap.servers': 'localhost:9092'})

# test_payload = {
#     "batch_id": f"group-{uuid.uuid4()}",
#     "chunk_index": 0,
#     "total_chunks": 1,
#     "entries": ["AAPL", "MSFT"],  # Your test symbols here
#     "posted_at": str(datetime.now().date())
# }

# producer.produce(
#     topic='market_mapping',
#     value=json.dumps(test_payload).encode('utf-8')
# )

# producer.flush()
# print("✅ Test message sent to Kafka.")

