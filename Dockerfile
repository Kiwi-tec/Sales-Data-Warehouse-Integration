FROM apache/airflow:2.7.1-python3.10

# Switch to root to install any system-level dependencies if needed
USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Switch back to airflow user to install python packages
USER airflow
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt