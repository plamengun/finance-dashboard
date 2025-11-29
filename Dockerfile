# Astro Runtime v9.2.0 = Airflow 2.9.0 + Python 3.12
FROM astrocrpublic.azurecr.io/runtime:3.0-8

# Install required Python packages
RUN pip install --no-cache-dir \
    clickhouse-connect==0.8.18

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt