# Crypto Data Engineering Project

This project is a work-in-progress data engineering pipeline for cryptocurrency data.  
The initial step focuses on **ingesting market data from the CoinGecko API** and saving it into a structured format (JSON/Parquet).  

## Current Progress
- ✅ CoinGecko ingestion script (`get_crypto_data`)  
- ✅ Data is saved into `data/bronze/` as JSON (raw layer / bronze layer)  
- ✅ Project initialized with Git for version control  

## Next Steps
- Add ingestion for Yahoo Finance and CoinDesk  
- Implement schema enforcement and transformations (silver layer)  
- Store cleaned data in PostgreSQL (gold layer)  
- Develop frontend to visualize crypto data  
- Extend with ML models for forecasting  

## Tech Stack
- Python (pandas, requests)  
- PostgreSQL (planned)  
- Git & GitHub for version control  
