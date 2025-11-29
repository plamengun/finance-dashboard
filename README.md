# Financial Data Engineering Pipeline

A real-time financial data engineering platform that extracts, processes, and streams market data using Apache Airflow, Apache Kafka, and ClickHouse.

## 🎯 What This App Does

This application is a **financial data pipeline** that:

1. **Extracts Market Data**: Fetches company information from external APIs (EODHD) for NASDAQ-listed companies
2. **Processes & Stores Data**: Ingests company data into ClickHouse database for analysis
3. **Streams Data**: Uses Kafka to stream processed data for real-time consumption
4. **Orchestrates Workflows**: Manages the entire data pipeline using Apache Airflow DAGs

### Key Features
- **Daily Market Data Ingestion** from EODHD API
- **Real-time Data Streaming** via Kafka topics
- **Columnar Data Storage** in ClickHouse for fast analytics
- **Automated Workflow Orchestration** with Airflow
- **Data Quality & Monitoring** with comprehensive logging

## 🏗️ Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   EODHD API     │───▶│  Apache Airflow │───▶│  ClickHouse DB  │
│  (NASDAQ Data)  │    │   (Orchestrator)│    │ (Data Warehouse)│
└─────────────────┘    └─────────────────┘    └─────────────────┘
                                │
                                ▼
                       ┌─────────────────┐
                       │  Apache Kafka   │
                       │  (Data Stream)  │
                       └─────────────────┘
                                │
                                ▼
                       ┌─────────────────┐
                       │  Data Consumers │
                       │  (Applications) │
                       └─────────────────┘
```

### Technology Stack
- **Orchestration**: Apache Airflow (via Astro CLI)
- **Message Broker**: Apache Kafka + Zookeeper
- **Database**: ClickHouse (columnar database)
- **Containerization**: Podman (Docker-compatible)
- **API Integration**: EODHD Financial API
- **Monitoring**: Kafka UI, Airflow UI

## 🚀 Local Setup

### Prerequisites
- **Windows 10/11** (tested on Windows 10.0.19045)
- **Podman** installed and running
- **Astro CLI** installed
- **PowerShell** (for Windows commands)

### 1. Install Dependencies

#### Install Astro CLI
```powershell
# Install Astro CLI
winget install Astronomer.AstroCLI
# OR download from: https://github.com/astronomer/astro-cli/releases
```

#### Install Podman
```powershell
# Install Podman Desktop
winget install RedHat.PodmanDesktop
# OR download from: https://podman-desktop.io/
```

### 2. Clone & Setup Project
```powershell
# Navigate to your workspace
cd D:\Programming\data_eng

# Verify Astro CLI
astro version

# Verify Podman
podman version
```

### 3. Environment Configuration

#### Set EODHD API Key
```powershell
# Set your EODHD API key in Airflow variables
astro dev run airflow variables set EODHD_API_KEY "your_api_key_here"
```

#### Verify Configuration Files
- `astro.yaml` - Astro CLI configuration
- `docker-compose.override.yml` - External services (Kafka, ClickHouse)
- `requirements.txt` - Python dependencies
- `Dockerfile` - Custom Airflow image

### 4. Start the Environment

#### Start Podman Machine (if needed)
```powershell
# Start Podman machine
podman machine start

# Set DOCKER_HOST for Astro CLI
$env:DOCKER_HOST = "npipe:////./pipe/docker_engine"
```

#### Start Development Environment
```powershell
# Start all services
astro dev start

# Wait for all services to be healthy
# This will start:
# - Airflow (webserver, scheduler, triggerer)
# - Kafka + Zookeeper
# - ClickHouse
# - Kafka UI
```

### 5. Access Services

| Service | URL | Port | Purpose |
|---------|-----|------|---------|
| **Airflow UI** | http://localhost:8080 | 8080 | Workflow orchestration |
| **Kafka UI** | http://localhost:8084 | 8084 | Kafka monitoring |
| **ClickHouse** | http://localhost:8123 | 8123 | Database HTTP interface |
| **Kafka** | localhost:9092 | 9092 | Kafka broker |

### 6. Verify Setup

#### Check Service Status
```powershell
# Check running containers
astro dev ps

# Check Airflow DAGs
# Open http://localhost:8080 in browser
# Login: airflow/airflow
```

#### Test Connections
```powershell
# Test ClickHouse connection
astro dev run airflow test connection clickhouse_default

# Test Kafka connection
astro dev run airflow test connection kafka_default
```

## 📊 Data Pipeline DAGs

### 1. `ingest_market_symbols_to_db_dag`
- **Schedule**: Daily
- **Purpose**: Fetches NASDAQ company data from EODHD API
- **Output**: Stores data in ClickHouse `nasdaq_companies` table

### 2. `ingest_market_symbols_to_kafka_dag`
- **Schedule**: Daily
- **Purpose**: Reads company symbols from ClickHouse and streams to Kafka
- **Output**: Kafka topic `market_mapping` with batched company data

### 3. `ingest_companies_info_to_kafka_dag`
- **Schedule**: Daily
- **Purpose**: Consumes company info from Kafka for processing
- **Output**: Processes streaming data from Kafka topics

## 🔧 Troubleshooting

### Common Issues

#### DNS Resolution Problems
```powershell
# If external API calls fail (e.g., eodhd.com)
# Check DNS configuration in astro.yaml
astro dev restart
```

#### Port Conflicts
```powershell
# If ports are already in use
netstat -ano | findstr :8080
netstat -ano | findstr :9092
```

#### Container Network Issues
```powershell
# Clean up and restart
astro dev stop
podman container prune -f
podman network prune -f
astro dev start
```

#### Database Connection Issues
```powershell
# Test ClickHouse connection
astro dev run airflow test connection clickhouse_default

# Check ClickHouse logs
podman logs clickhouse
```

### Reset Environment
```powershell
# Complete reset
astro dev stop
podman system prune -a -f
astro dev start
```

## 📁 Project Structure

```
data_eng/
├── dags/                           # Airflow DAGs
│   ├── ingest_market_symbols_to_db_dag.py      # API → ClickHouse
│   ├── ingest_market_symbols_to_kafka_dag.py   # ClickHouse → Kafka
│   ├── ingest_companies_info_to_kafka_dag.py   # Kafka consumer
│   ├── db_utils.py                 # Database utilities
│   └── db_conn_dag.py             # Connection testing
├── docker-compose.override.yml     # External services config
├── astro.yaml                      # Astro CLI configuration
├── requirements.txt                 # Python dependencies
├── Dockerfile                      # Custom Airflow image
└── README.md                       # This file
```

## 🚀 Next Steps

1. **Set your EODHD API key** in Airflow variables
2. **Start the environment** with `astro dev start`
3. **Monitor DAGs** in Airflow UI at http://localhost:8080
4. **Check data flow** in Kafka UI at http://localhost:8084
5. **Query ClickHouse** for stored data

## 📚 Additional Resources

- [Astro CLI Documentation](https://docs.astronomer.io/astro/cli/)
- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [ClickHouse Documentation](https://clickhouse.com/docs/)
- [EODHD API Documentation](https://eodhd.com/apis)

## 🤝 Support

For issues or questions:
1. Check the troubleshooting section above
2. Review container logs: `podman logs <container_name>`
3. Check Airflow task logs in the UI
4. Verify network connectivity between services

---

**Happy Data Engineering! 🎉**
