## Crypto App

A pragmatic, extensible crypto data platform: ingest, store, orchestrate, and visualize market data. Built for iteration and future growth (ML + modern frontend).

### Highlights
- **Sources**: Yahoo Finance, CoinDesk, CoinGecko
- **Pipelines**: Apache Airflow (hourly/daily)
- **Storage**: PostgreSQL
- **Dashboards**: Streamlit (now), room for Next.js later
- **Language**: Python

### Tech Stack
- Airflow (containerized)
- PostgreSQL (containerized)
- Python: yfinance, pandas, psycopg2-binary, python-dotenv
- Streamlit (local dashboards)

### Repository Layout
```
crypto-app/
├─ airflow-docker/
│  ├─ docker-compose.yaml         # Airflow + Postgres stack
│  └─ dags/                       # Airflow DAGs (yf_historical, yf_hourly, coingecko, etc.)
├─ data/
│  └─ bronze/                     # Raw files (e.g., coingecko JSON)
├─ yf_historical.py               # Historical Yahoo Finance → Postgres
├─ yf_hourly.py                   # Hourly Yahoo Finance → Postgres
├─ coindesk_dashboard.py          # Streamlit dashboard (example)
├─ coingecko_ingest_postgres.py   # Ingest CoinGecko → Postgres
├─ app.py                         # Additional app entry (if needed)
└─ readme.md
```

### Quickstart
1) Clone
```bash
git clone https://github.com/Bensonn5151/crypto-app.git
cd crypto-app
```

2) Environment
- Create a `.env` at repo root with your DB creds if running scripts locally. Example:
```bash
DB_HOST=localhost
DB_PORT=5432
DB_NAME="" #crypto_app
DB_USERNAME="" #postgres
DB_PASSWORD= ""
```
- The Airflow stack mounts `../.env` into the container at `/opt/airflow/.env`.

3) Run the Airflow stack
```bash
cd airflow-docker
# Start services
docker compose up -d
# View web UI
open http://localhost:8080
```
- Airflow admin user/password are set in `docker-compose.yaml`.
- Postgres service is exposed on `localhost:5432` (see compose for creds).

4) Trigger pipelines
- In Airflow UI, unpause and trigger:
  - `yf_historical__dag`
  - `yf_hourly__dag` 
  - `coingecko__dag`

5) Run scripts locally (optional)
```bash
# In a local Python environment
pip install yfinance pandas psycopg2-binary python-dotenv streamlit
python yf_historical.py
python yf_hourly.py
streamlit run coindesk_dashboard.py
```

### Notes on Dependencies (containers)
- The Airflow image installs Python deps via `_PIP_ADDITIONAL_REQUIREMENTS` in `airflow-docker/docker-compose.yaml`.
- If you change dependencies, restart the stack:
```bash
cd airflow-docker
docker compose down
docker compose up -d
```

### Data & Database
- Raw JSON (bronze) stored under `data/bronze/...`
- Postgres schema (example): `yfinance_historical` for OHLCV data
- Scripts enforce light schema before load (see `yf_historical.py`)

### Troubleshooting
- Import errors inside Airflow (e.g., `ModuleNotFoundError: yfinance`):
  - Ensure `_PIP_ADDITIONAL_REQUIREMENTS` includes your packages
  - Restart the Airflow stack after edits to `docker-compose.yaml`
- Import errors for local scripts: `pip install` the packages listed above and ensure `.env` is present
- DB connection issues: verify compose DB creds match your `.env`

### Roadmap (Vertical Scaling)
- ML/Analytics
  - Price forecasting (Prophet / LSTM) and backtesting
  - Volatility clustering, anomaly/spike detection
  - Feature store for engineered factors (returns, momentum, on-chain when available)
- Data Platform
  - Bronze → Silver → Gold layers; add validations and SLAs
  - Incremental loads, CDC, and partitioning
  - Warehouse integration (DuckDB/BigQuery/Snowflake optional)
- Frontend
  - Next.js dashboard with SSR/ISR and API routes
  - Rich charts (light/dark mode), alerts, and portfolio views
  - Auth + user workspaces, saved queries, and watchlists
- Ops
  - CI/CD for DAGs and analytics
  - Observability (OpenLineage/Marquez), data quality checks

### License
MIT (or choose your preferred license).
