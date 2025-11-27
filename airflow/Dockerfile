FROM apache/airflow:2.10.2

USER root

# Install Postgres client (needed for pg_isready) + build tools
RUN apt-get update && apt-get install -y \
    postgresql-client \
    build-essential \
    python3-dev \
    libssl-dev \
    libffi-dev \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Install Python libraries
RUN pip install --no-cache-dir \
    openmeteo-requests \
    requests-cache \
    retry-requests \
    numpy \
    pandas

