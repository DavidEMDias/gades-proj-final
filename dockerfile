FROM python:3.11-slim

# Dependências do sistema necessárias para dbt e BigQuery
RUN apt-get update && apt-get install -y \
    build-essential \
    git \
    curl \
    libssl-dev \
    libffi-dev \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/dbt

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt
