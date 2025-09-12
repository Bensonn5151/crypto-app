# Crypto Data Engineering Project
This project is a work-in-progress data engineering pipeline for cryptocurrency data.
It follows a layered architecture (Bronze → Silver → Gold) for structured, scalable, and reliable data workflows.

## Current Progress
✅ CoinGecko ingestion script (get_crypto_data)
✅ Data saved into data/bronze/ as JSON (raw layer / bronze layer)
✅ Project initialized with Git for version control
✅ Yahoo Finance ingestion added
✅ CoinDesk ingestion added
✅ Schema enforcement and transformations implemented (Silver Layer)

## Next Steps
Store cleaned & transformed data in PostgreSQL (Gold Layer)
Develop a frontend dashboard to visualize crypto data
Extend pipeline with ML models for price forecasting & anomaly detection

## Tech Stack
Python (pandas, requests, yfinance)
PostgreSQL (planned for Gold Layer)
Git & GitHub for version control
Data Architecture: Bronze (raw) → Silver (clean) → Gold (analytics-ready)
