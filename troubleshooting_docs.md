# Troubleshooting Log - Crypto Data Pipeline

This document records specific errors encountered and their solutions for future reference.

## Error Log

### 🚨 Error 001: "Data not visible in PostgreSQL after successful DAG run"
**Symptoms**: 
- Airflow logs show: `✅ Data ingested to Postgres table 'silver_coingecko'`
- No data visible in pgAdmin
- Table exists but is empty

**Root Cause**: 
- Local PostgreSQL running on port 5432
- Docker PostgreSQL also using port 5432
- pgAdmin connecting to local PostgreSQL instead of Docker PostgreSQL

**Solution**:
1. Changed Docker port mapping to avoid conflict:
   ```yaml
   # docker-compose.yml
   services:
     postgres:
       ports:
         - "5434:5432"  # Host:Container

2. Updated pgAdmin connection to use port 5434

3. Kept .env file with container port 5432
Verification: 
  docker exec -it airflow-docker-postgres-1 psql -U postgres -d crypto_app -c "SELECT COUNT(*) FROM silver_coingecko;"

### 🚨 Error 002: "Connection not found" in Airflow

Symptoms:

DAG fails with connection errors
airflow connections get postgres_default returns "Connection not found"
Solution:
  # Recreate the connection
airflow connections delete postgres_default
airflow connections add postgres_default \
    --conn-uri postgresql://postgres:bens@postgres:5432/crypto_app
